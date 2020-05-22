package com.andrew.sparkwork.trans;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractTransformer implements Transformer, Serializable {

	protected Properties props;
	
	public AbstractTransformer( Properties p){
		props = p;
	}
	
	@Override
	public abstract Map<String,Dataset<Row>> transform(Map<String, Dataset<Row>> map, SparkSession session, String stepName) throws Exception;

	@Override
	public void setProperty(String key, String value) {
		props.setProperty(key, value);
	}
	
	public String resolveDelimiter( String del ) 
	{
		if( del.startsWith("chars:")) {
			String[] codeArray = del.substring( "chars:".length() ).split(",");
			
			StringBuilder builder = new StringBuilder();
			
			for( String c : codeArray ) {
				builder.append(Character.toChars(Integer.parseInt(c)));
			}
			
			return builder.toString();
		} else {
			return del;
		}
	}

}