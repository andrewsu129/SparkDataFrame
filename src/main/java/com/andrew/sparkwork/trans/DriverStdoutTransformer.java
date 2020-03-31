package com.andrew.sparkwork.trans;

import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DriverStdoutTransformer extends ExternalOutputTransformer {

	public DriverStdoutTransformer( Properties p) 
	{
		super(p);
	}
	
	@Override
	public void write(Dataset<Row> dataframe, SparkSession session) throws Exception {


		List<Row> list = dataframe.toJavaRDD().collect();
		
		for( Row row : list ) {
			System.out.println( row.mkString("Row {", ",", "}"));
		}
	}

}
