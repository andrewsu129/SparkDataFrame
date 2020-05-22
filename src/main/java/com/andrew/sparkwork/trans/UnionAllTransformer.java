package com.andrew.sparkwork.trans;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnionAllTransformer extends AbstractTransformer 
{

	public UnionAllTransformer(Properties p)
	{
		super(p);
	}
	
	@Override
	public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> map, SparkSession session, String stepName) throws Exception {

		Iterator<Map.Entry<String, Dataset<Row>>> iterator = map.entrySet().iterator();
		
		Dataset<Row> unioned = iterator.next().getValue();
		
		while( iterator.hasNext() ) {
			unioned = unioned.union(iterator.next().getValue());
		}

		Map<String, Dataset<Row>> dataFrameMap = new HashMap<>();
		dataFrameMap.put(stepName, unioned);
		return dataFrameMap;
	}

}
