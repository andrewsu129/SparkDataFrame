package com.andrew.sparkwork.trans;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadHiveTransformer extends ExternalInputTransformer {

	public ReadHiveTransformer(Properties p ) {
		super(p);
	}
	
	@Override
	public Dataset<Row> read(SparkSession session) throws Exception {
		String table = props.getProperty("table");
		
		Dataset<Row> dataframe = session.table(table);
		return dataframe;
	}

}
