package com.andrew.sparkwork.trans;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JdbcInputTransformer extends ExternalInputTransformer {

	public JdbcInputTransformer(Properties p ) {
		super(p);
	}
	
	@Override
	public Dataset<Row> read(SparkSession session) throws Exception {
		String table = props.getProperty("table");
		String url = props.getProperty("url");
		
		Dataset<Row> dataframe = session.read().jdbc(url, table, props);
		return dataframe;
	}

}
