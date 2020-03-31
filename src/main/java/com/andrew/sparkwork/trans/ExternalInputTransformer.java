package com.andrew.sparkwork.trans;

import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public abstract class ExternalInputTransformer extends AbstractTransformer {

	private static final Logger log = LoggerFactory.getLogger(ExternalInputTransformer.class);
	
	public ExternalInputTransformer(Properties p) 
	{
		super( p);
	}
	
	@Override
	public Dataset<Row> transform(Map<String, Dataset<Row>> inputs, SparkSession session) throws Exception
	{
		if( inputs != null ) {
			log.info("Total inputs: {}", inputs.size());
		}
		
		if( inputs.size() != 0 ) {
			throw new RuntimeException("Souce transfomer can not have any dependencies");
		}
		
		return read(session);
	}

	public abstract Dataset<Row> read( SparkSession session ) throws Exception;
}
