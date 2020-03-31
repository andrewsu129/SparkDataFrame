package com.andrew.sparkwork.trans;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Transformer {

	Dataset<Row> transform(Map<String, Dataset<Row>> map,  SparkSession session) throws Exception;
	
	void setProperty(String key, String value);
}
