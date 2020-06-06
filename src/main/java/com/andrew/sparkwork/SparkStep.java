package com.andrew.sparkwork;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import com.andrew.sparkwork.trans.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.andrew.sparkwork.utils.PropertyUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


import com.google.common.collect.Lists;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.LogManager;
import org.apache.spark.sql.Dataset;

public class SparkStep {
	private static final Logger log = LoggerFactory.getLogger(SparkStep.class);
	
	private String name;
	private Properties props;
	private Map<String, Dataset<Row>> dataFrameMap;
	private boolean processed;
	
	private Transformer sourceTransformer;
	private Transformer sinkTransformer;

	private List<String> loopValues;
	private int loopCount = -1;
	
	private String copyParameter;
	
	public SparkStep(String name, Properties props) throws Exception 
	{
		this.name = name;
		this.props = props;
		processed = false;

		dataFrameMap = new HashMap<>();
		loadTransformer();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}
	
	public void setProperty(String key, String value) 
	{
		props.setProperty(key, value);
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
		log.info("Setting processed for {} to {}", getName(), processed);
	}

	public Transformer getSourceTransformer() {
		return sourceTransformer;
	}

	public void setSourceTransformer(Transformer sourceTransformer) {
		this.sourceTransformer = sourceTransformer;
	}

	public Transformer getSinkTransformer() {
		return sinkTransformer;
	}

	public void setSinkTransformer(Transformer sinkTransformer) {
		this.sinkTransformer = sinkTransformer;
	}
	
	public boolean hasSourceTransformer()
	{
		return sourceTransformer != null;
	}
	
	public boolean hasSinkTransformer()
	{
		return sinkTransformer != null;
	}
	
	public boolean doesCopyIntoParameter()
	{
		return props.containsKey("source.copy.into.parameter");
	}
	
	private String getCopyParamterFor(Dataset<Row> output)
	{
		if( output.columns().length !=1 ) {
			throw new RuntimeException("Copy into Paramter only supports one column source inputs");
		}
		
		Row[] rows = (Row[]) output.collect();
		
		StringBuilder sb = new StringBuilder();
		
		for( Row row : rows ) {
			sb.append(row.get(0));
			sb.append(",");
		}
		
		sb.setLength(sb.length() - 1 );
		String value = sb.toString();
		
		log.info("The value of copyIntoParameter: {}", value);
		
		return value;
	}

	public boolean hasDependencies() 
	{
		return getDependenciesName() != null;
	}
	
	public Set<String> getDependenciesName()
	{
		String dependenciesString = props.getProperty("dependencies");
		if( dependenciesString != null ) {
			return Sets.newHashSet( dependenciesString.split(Pattern.quote(",")));
		} else {
			return Sets.newHashSet();
		}
	}

	public Map<String, Dataset<Row>> getDataFrameMap() {
		return dataFrameMap;
	}

	public void setDataFrameMap(Map<String, Dataset<Row>> dataFrame) {
		this.dataFrameMap = dataFrameMap;
	}
	
	public boolean hasDataFrame() 
	{
		//return dataFrame != null;
		return !dataFrameMap.isEmpty();
	}
	
	public boolean doesLoop()
	{
		return Boolean.parseBoolean(props.getProperty("loop", "false"));
	}
	
	public boolean hasMoreToLoop()
	{
		return loopCount < loopValues.size() - 1;
	}
	
	public void nextLoop()
	{
		loopCount += 1;
		
		reloadLoopValues();
	}
	
	public Map<String, String> getGlobalPropertyOverrides()
	{
		Map<String, String> overrides = new HashMap<String, String>();
		
		if( doesLoop() && loopCount > -1 ) {
			String loopParameter = "parameter." + props.getProperty("loop.parameter");
			overrides.put(loopParameter, loopValues.get(loopCount));
		}
		
		if( doesCopyIntoParameter() ) {
			String copyParameterName = "parameter." + props.getProperty("source.copy.into.parameter");
			overrides.put(copyParameterName, copyParameter);
		}
		
		return overrides;
	}
	
	public void runSparkStep(Map<String, Dataset<Row>> inputs, SparkSession session) throws Exception
	{
		System.out.println("Running Spark Step: " + getName() );
		log.info("Running Spark Step: " + getName() );
		
		if( doesLoop() ) {
			nextLoop();
		} else {
			runTransformer(inputs, session);
		}
		
		this.setProcessed(true);
	}
	
	public void runTransformer(Map<String, Dataset<Row>> inputs, SparkSession session) throws Exception 
	{
		Transformer transformer = this.getSourceTransformer();
		
		long start = System.currentTimeMillis();
		
		Map<String, Dataset<Row>> output = transformer.transform(inputs, session, getName());
		
		if( Boolean.parseBoolean(props.getProperty("cache", "false"))) {
			output.forEach( (x,y) -> y.cache() ) ;
		}
		
		if( this.doesCopyIntoParameter() ) {
			output.forEach( (x,y) -> this.getCopyParamterFor(y));
		}

		dataFrameMap = output;

		dataFrameMap.forEach( (x,y) -> y.createOrReplaceTempView(x) );
		
		
		if( this.hasSinkTransformer() ) {
			Transformer sinkTransformer = this.getSinkTransformer();
			
			start = System.currentTimeMillis();
			sinkTransformer.transform(dataFrameMap, session, getName());
			if(Boolean.parseBoolean(props.getProperty("parameter.application.debug", "false"))) {
				long end = System.currentTimeMillis();
				String title = "Time taken to execute: " + props.getProperty("sink") + " of " + name + " :";
				flushLogs(start, end, title);
			}
		}
	}
	
	public void loadTransformer() throws Exception 
	{
		//log.info( "THE PROPERTIES IS: {}", props);
		if(props.containsKey("source" ) ) {
			String transformerName = props.getProperty("source");
			
			Properties transformerProperties = PropertyUtils.propertiesForPrefix(props, "source");

			switch( transformerName ) {
				case "sql":
					this.sourceTransformer = new SQLQueryTransformer(transformerProperties);
					break;
				case "custom":
					String className = props.getProperty("source.class");
					Class<?> clazz = Class.forName(className);

					Constructor<?> constructor = clazz.getConstructor(Properties.class);
					this.sourceTransformer = (Transformer) constructor.newInstance(transformerProperties);
					break;
				case "hdfs":
					this.sourceTransformer = new ReadHDFSTransformer(transformerProperties);
					break;
				case "local":
					this.sourceTransformer = new ReadHDFSTransformer(transformerProperties);
					break;
				case "hive":
					this.sourceTransformer = new ReadHiveTransformer(transformerProperties);
					break;
				case "jdbc":
					this.sourceTransformer = new JdbcInputTransformer(transformerProperties);
					break;
				case "dataframe_split":
					this.sourceTransformer = new RandomSplitDataframeTransformer(transformerProperties);
					break;
				case "mllib":
					this.sourceTransformer = new MLLibTransformer(transformerProperties);
					break;
				case "simpleRandomForest":
					this.sourceTransformer = new SimpleRandomForestTransformer(transformerProperties);
					break;
				case "simpleGradientBoostedTree":
					this.sourceTransformer = new SimpleGradientBoostedTreeTransformer(transformerProperties);
					break;
				case "simpleNaiveBayes":
					this.sourceTransformer = new SimpleNaiveBayesTransformer(transformerProperties);
					break;
				case "simpleLogisticRegression":
					this.sourceTransformer = new SimpleLogisticRegressionTransformer(transformerProperties);
					break;
				default:
					throw new RuntimeException("Unsupported source transformer type: " + transformerName );
			}

		}
		
		if( props.containsKey("sink")) {
			String transformerName = props.getProperty("sink");
			Properties transformerProperties = PropertyUtils.propertiesForPrefix(props, "sink");

			switch( transformerName ) {
				case "hdfs":
					this.sinkTransformer = new WriteHDFSTransformer(transformerProperties);
					break;
				case "local":
					this.sinkTransformer = new WriteLocalFileTransformer(transformerProperties);
					break;
				case "hive":
					this.sinkTransformer = new WriteHiveTransformer(transformerProperties);
					break;
				case "stdout":
					this.sinkTransformer = new DriverStdoutTransformer(transformerProperties);
					break;
				case "jdbc":
					this.sinkTransformer = new JdbcOutputTransformer(transformerProperties);
					break;
				default:
					throw new RuntimeException("Unsupported sink transformer type: " + transformerName);
			}
			
		}
	}
	
	public void reloadLoopValues()
	{
		String loopType = props.getProperty("loop.type");
		
		if( loopType.equals("range")) {
			long rangeFrom = Long.parseLong(props.getProperty("loop.type.from"));
			long rangeTo = Long.parseLong(props.getProperty("loop.type.to"));
			
			loopValues = Lists.newArrayList();
			
			for( long i=rangeFrom; i <= rangeTo; i++ ) {
				loopValues.add( String.valueOf(i));
			}
		} else if( loopType.equals("list")) {
			String listValuesString = props.getProperty("loop.list.values");
			String filledListValuesString = PropertyUtils.fillParameters(listValuesString, props);
			
			loopValues = Arrays.asList(filledListValuesString.split(Pattern.quote(",")));
		}
	}
	
	private void flushLogs(long start, long end, String title)
	{
		Logger tLog = LoggerFactory.getLogger(SparkStep.class);
		
		tLog.info("Performance Metrix: BEGIN");
		tLog.info("{} {} seconds", title, (end-start)/1000);
		tLog.info("Performance Metrix: END");
	}
}
