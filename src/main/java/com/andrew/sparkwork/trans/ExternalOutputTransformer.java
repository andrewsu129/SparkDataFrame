package com.andrew.sparkwork.trans;

import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public abstract class ExternalOutputTransformer  extends AbstractTransformer {

	private static final Logger log = LoggerFactory.getLogger(ExternalOutputTransformer.class);
	
	public ExternalOutputTransformer(Properties p) 
	{
		super( p);
	}

	@Override
	public Dataset<Row> transform(Map<String, Dataset<Row>> inputs, SparkSession session) throws Exception
	{
		if( inputs.size() != 1 ) {
			throw new RuntimeException("Sink output transformer can have only one data frame ");
		}
		
		Dataset<Row> dataframe = inputs.entrySet().iterator().next().getValue();
		
		write(dataframe, session);
		
		return null;
	}
	
	
	public abstract void write(Dataset<Row> dataframe, SparkSession session) throws Exception;

}
