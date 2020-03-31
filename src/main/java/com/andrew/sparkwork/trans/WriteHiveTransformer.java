package com.andrew.sparkwork.trans;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.andrew.sparkwork.utils.PropertyUtils;

public class WriteHiveTransformer extends ExternalOutputTransformer
{
	public WriteHiveTransformer(Properties p)
	{
		super(p);
	}
	
	@Override
	public void write(Dataset<Row> dataframe, SparkSession session) throws Exception {
		String tableName = props.getProperty("table");
		String filledTableString = PropertyUtils.fillParameters(tableName, props);
		
		SaveMode mode = SaveMode.valueOf(props.getProperty("mode", "Overwrite"));
		
		DataFrameWriter writer = dataframe.write();
		if( Boolean.parseBoolean(props.getProperty("do.partition", "false"))) {
			String[] partitionColumns = props.getProperty("partition.columns")
					.split(Pattern.quote(","));
			
			writer.partitionBy(partitionColumns);
		}
		
		writer.mode(mode).saveAsTable(filledTableString);
		
	}

}
