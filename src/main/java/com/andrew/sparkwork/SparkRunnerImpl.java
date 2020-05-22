package com.andrew.sparkwork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.andrew.sparkwork.utils.PropertyUtils;

public class SparkRunnerImpl {
	
	private static final Logger log = LoggerFactory.getLogger(SparkRunnerImpl.class);
	
	public static void SparkRun( final SparkSession session, Properties props ) throws Exception
	{
		setUserHomeDirectory( props );
		
		final Set<SparkStep> steps;
		if( Boolean.parseBoolean(props.getProperty("job.restart", "false"))) {
			String preStep = props.getProperty("job.restart.from.step");
			int loopValue = Integer.parseInt(props.getProperty("job.restart.loop.value", "0"));
			if( preStep == null ) {
				System.exit( -1 );
			} 
			steps = changePropsForRerun( preStep, loopValue, props );
			
		} else {
			steps = initiateSteps(props);
		}
		
		int numThreads = Runtime.getRuntime().availableProcessors();
		ExecutorService tpool = Executors.newFixedThreadPool(numThreads);
		System.out.println("SP>> Thread pool created with " + numThreads + " threads" );
		log.info("SP>> Thread pool created with " + numThreads + " threads" );
		
		while(!allStepProcessed(steps))
		{
			List<Future<Boolean>> baseLoop = new ArrayList<>();
			
			for(final SparkStep step : steps ) {
				if( !step.isProcessed() ) {
					Set<String> dependencyNames = step.getDependenciesName();
					final Set<SparkStep> dependencies = stepsHavingName(steps, dependencyNames);
					
					if( allStepProcessed(dependencies)) {
						final Map<String, Dataset<Row>> dependencyDataFrames = extractDataFrames(dependencies);
						Callable<Boolean> callable = new Callable<Boolean>() {

							@Override
							public Boolean call() throws Exception {
								step.runSparkStep(dependencyDataFrames, session);
								applyGlobalPropertyOverwrites(step.getGlobalPropertyOverrides(), steps);
								return true;
							}
							
						};
						baseLoop.add(tpool.submit(callable));
					}										
				}
			}
			
			System.out.println("SP>> There are " + baseLoop.size() + " Future Objects created for the first loop");
			log.info("SP>> There are " + baseLoop.size() + " Future Objects created for the first loop");
			
			List<Boolean> blocking_results = new ArrayList<>();
			for(Future<Boolean> trd : baseLoop ) {
				blocking_results.add(trd.get());
			}
			
			baseLoop.clear();
			
			for( SparkStep step : steps ) {
				if( step.doesLoop() ) {
					Set<SparkStep> dependencySteps = extractDependentSteps(step, steps);
					
					if( allStepProcessed(dependencySteps) && step.hasMoreToLoop() ) {
						step.setProcessed(false);
						for( SparkStep dependencyStep : dependencySteps ) {
							dependencyStep.setProcessed(false);
						}
					}
				}
			}
		}
		
		
		tpool.shutdown();
	}
	
	public static void setUserHomeDirectory( Properties props ) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration());
		
		String homeDir = fs.getHomeDirectory().toString();
		String dirPattern = "${user_home_dir}";
		for( String prop : props.stringPropertyNames() ) {
			String value = props.getProperty(prop);
			if( value.contains(dirPattern)) {
				value = value.replaceAll(dirPattern, homeDir);
				props.setProperty(prop, value);
				System.out.println("SPARK>>>> User Home Directory that will be used for getting the file is :" + value);
			}
		}
		
	}
	
	private static void applyGlobalPropertyOverwrites(Map<String, String> overrides, Set<SparkStep> steps)
	{
		for( SparkStep step : steps ) {
			for( Map.Entry<String, String> override : overrides.entrySet() ) {
				String overrideKey = override.getKey();
				String overrideValue = override.getValue();
				
				step.setProperty(overrideKey, overrideValue);
				
				if( step.hasSourceTransformer() ) {
					step.getSourceTransformer().setProperty(overrideKey, overrideValue);
				}
				if( step.hasSinkTransformer() ) {
					step.getSinkTransformer().setProperty(overrideKey, overrideValue);
				}
			}
		}
	}
	
	private static Set<SparkStep> extractDependentSteps(SparkStep dependency,  Set<SparkStep> steps ) throws Exception
	{
		Set<SparkStep> dependencies = new HashSet<>();
		
		for( SparkStep step : steps ) {
			if( step.getDependenciesName().contains(dependency.getName()) ) {
				dependencies.add(step);
			}
		}
		
		
		return dependencies;
	}
	
	private static Set<SparkStep> initiateSteps(Properties props) throws Exception
	{
		Set<SparkStep> steps = new HashSet<>();
		String[] stepNames = props.getProperty("steps").split(Pattern.quote(","));
		
		for( String stepName : stepNames ) {
			String prefix = "step." + stepName + ".";
			Properties stepProperties = PropertyUtils.propertiesForPrefix(props, prefix);
			
			SparkStep step = new SparkStep(stepName, stepProperties);
			steps.add(step);
		}
		
		
		return steps;
		
	}
	
	private static Set<SparkStep> changePropsForRerun(String preStep, int preLoopVal, Properties props) throws Exception
	{
		
		String prefix = "step." + preStep + ".";
		Properties stepProperties = PropertyUtils.propertiesForPrefix(props, prefix);
		if( !stepProperties.contains("sink") ) {
			System.exit(1);
		}
		
		Set<String> checkList = new HashSet<>();
		Stack<String> dependencies = new Stack<>();
		dependencies.push(preStep);
		Set<SparkStep> steps = new HashSet<>();
		
		while( !dependencies.isEmpty()) {
			String dependency = dependencies.pop();
			if( !checkList.contains(dependency)) {
				checkList.add(dependency);
				String dependencyPrefix = "step." + dependency + ".";
				Properties dependencyProps = PropertyUtils.propertiesForPrefix(props, dependencyPrefix);
				String dependencyString = dependencyProps.getProperty("dependencies");
				if( dependencyString != null ) {
					String[] ss = dependencyString.split(Pattern.quote(","));
					for( String s : ss) {
						dependencies.push(s);
					}
				}
			}
		}
		
		String[] stepNames = props.getProperty("steps").split(Pattern.quote(","));
		for( String stepName : stepNames ) {
			prefix = "step." + stepName + ".";
			stepProperties = PropertyUtils.propertiesForPrefix(props, prefix);
			
			if( checkList.contains(stepName)) {
				if( stepProperties.contains("sink") ) {
					stepProperties.setProperty("source", stepProperties.getProperty("sink"));
					stepProperties.setProperty("source.table", stepProperties.getProperty("sink.table"));
					stepProperties.remove("sink");
				}
			}
			
			SparkStep step = new SparkStep(stepName, stepProperties);
			steps.add(step);

		}
		
		return steps;
	}
	
	private static Map<String, Dataset<Row>> extractDataFrames( Set<SparkStep> steps ) 
	{
		Map<String, Dataset<Row>> dataFrameMap = new HashMap<>();
		for( SparkStep step : steps ) {
			//dataFrameMap.put(step.getName(), step.getDataFrame());
			dataFrameMap.putAll(step.getDataFrameMap());
		}
		
		return dataFrameMap;
	}
 	
	private static Set<SparkStep> stepsHavingName(Set<SparkStep> steps, Set<String> dependencyNames)
	{
		Set<SparkStep> nameSteps = new HashSet<>();
		for( SparkStep step : steps) {
			if( dependencyNames.contains(step.getName())) {
				nameSteps.add(step);
			}
		}
		return nameSteps;
	}
	
	private static boolean allStepProcessed(Collection<SparkStep> steps) 
	{
		for( SparkStep step : steps ) {
			if( !step.isProcessed() ) {
				return false;
			}
		}
		
		return true;
	}
}