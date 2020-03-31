package com.andrew.sparkwork.trans;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.andrew.sparkwork.utils.PropertyUtils;

public class JdbcOutputTransformer extends ExternalOutputTransformer
{
	public JdbcOutputTransformer(Properties p)
	{
		super(p);
	}
	
	@Override
	public void write(Dataset<Row> dataframe, SparkSession session) throws Exception {
		String tableName = props.getProperty("table");
		String url = props.getProperty("url");
		
		SaveMode mode = SaveMode.valueOf(props.getProperty("mode", "Overwrite"));
		
		DataFrameWriter writer = dataframe.write();
		
		
		writer.mode(mode).jdbc(url, tableName, props);
		
	}

}
