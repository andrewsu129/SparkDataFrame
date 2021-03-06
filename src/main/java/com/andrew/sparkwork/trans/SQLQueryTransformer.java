package com.andrew.sparkwork.trans;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.andrew.sparkwork.utils.HDFSUtils;
import com.andrew.sparkwork.utils.PropertyUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLQueryTransformer extends AbstractTransformer
{
	
	public SQLQueryTransformer(Properties p)
	{
		super(p);
	}

	@Override
	public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> map, SparkSession session, String stepName) throws Exception {
		String rawQuery = getRawQuery(props);
		String finalQuery = PropertyUtils.fillParameters(rawQuery, props);
		
		System.out.println("The final Query is : " + finalQuery);
		
		Map<String, Dataset<Row>> dataFrameMap = new HashMap<>();
		dataFrameMap.put(stepName, session.sql(finalQuery));
		
		return dataFrameMap;
	}
	
	private String getRawQuery(Properties props) throws Exception
	{
		String query = null;
		
		if( props.containsKey("query.literal")) {
			query = props.getProperty("query.literal");
		} else if( props.containsKey("query.file")) {
			query = HDFSUtils.hdfsFileAsString(props.getProperty("query.file"));
		} else {
			throw new RuntimeException("Unsupported query type for SQL transformer");
		}
		
		return query;
	}
	
}
