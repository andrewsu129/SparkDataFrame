package com.andrew.sparkwork.trans;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.andrew.sparkwork.utils.PropertyUtils;
import com.andrew.sparkwork.utils.HDFSUtils;

public class WriteHDFSTransformer extends ExternalOutputTransformer
{
	
	public WriteHDFSTransformer(Properties p)
	{
		super(p);
	}

	@Override
	public void write(Dataset<Row> dataframe, SparkSession session) throws Exception {
		boolean asSingleFile = Boolean.parseBoolean(props.getProperty("single.file", "false"));
		
		if( asSingleFile ) {
			dataframe = dataframe.coalesce(1); 
		}
		
		dataframe = sortData(dataframe);
		
		String format = props.getProperty("format");
		switch(format)
		{
		case "parquet":
			writeParquet(dataframe);
			break;
		case "text":
			writeText(dataframe);
			break;
		default:
			throw new RuntimeException("Unsupported sink format: " + format);
		}

	}
	
	private void writeParquet(Dataset<Row> dataframe) 
	{
		String rawPath = props.getProperty("path");
		String outputPath = PropertyUtils.fillParameters(rawPath, props) ;	
		
		SaveMode saveMode = SaveMode.valueOf(props.getProperty("mode", "Overwrite"));
		
	}
	
	private void writeText(Dataset<Row> dataframe) throws Exception
	{
		JavaRDD<Row> rawRecords = dataframe.toJavaRDD();
		final String delimiter = props.getProperty("delimiter");
		
	
		JavaRDD<String> stringRecords = rawRecords.map(new Function<Row, String>() {

			StringBuilder text = new StringBuilder();
			
			@Override
			public String call(Row arg0) throws Exception {
				
				text.setLength(0);
				
				for( String fieldName : arg0.schema().fieldNames() ) {
					if( !arg0.isNullAt(arg0.fieldIndex(fieldName))) {
						String valueString = arg0.get(arg0.fieldIndex(fieldName)).toString();
						text.append(valueString);
					}
					
					text.append(resolveDelimiter(delimiter));
				}
				
				if( text.length() > delimiter.length() ) {
					text.setLength(text.length() - resolveDelimiter(delimiter).length());
				}
				
				return text.toString();
			}
			
		});
		
		String rawPath = props.getProperty("path");
		String outputPath = PropertyUtils.fillParameters(rawPath, props) ;	
		
		HDFSUtils.removeHDFSPath(outputPath);
		
		stringRecords.saveAsTextFile(outputPath);
		
		
	}
	
	private Dataset<Row> sortData(Dataset<Row> dataframe) throws Exception
	{
		final String sortColumnsProp = props.getProperty("sort.columns", "");
		
		if( ! "".equals(sortColumnsProp.trim())) {
			String[] sortColumnArray = sortColumnsProp.split(",");
			Column[] sortColumns = new Column[sortColumnArray.length]; 
			int index = 0;
			
			for( String sortColumn : sortColumnArray ) {
				if( sortColumn.indexOf(":") > 0 ) {
					String[] sortOrderArray = sortColumn.split(":");
					if( "desc".equalsIgnoreCase(sortOrderArray[1])) {
						sortColumns[index] = dataframe.col(sortOrderArray[0]).desc();
					} else {
						sortColumns[index] = dataframe.col(sortOrderArray[0]).asc();
					}
					
					System.out.println("Sort Column " + index + " is " + sortOrderArray[0]);
				} else {
					sortColumns[index] = dataframe.col(sortColumn).asc();
					
					System.out.println("Sort Column " + index + " is " + sortColumn);
				}
				
				index++;
			}
			
			dataframe = dataframe.sort(sortColumns);
		}
		
		return dataframe;
		
	}

}
