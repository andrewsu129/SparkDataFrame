package com.andrew.sparkwork.trans;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.andrew.sparkwork.utils.HDFSUtils;
import com.andrew.sparkwork.utils.PropertyUtils;


public class ReadHDFSTransformer extends ExternalInputTransformer {

	/**
	 * 
	 */
	private static final long serialVersionUID = 789820193908299610L;

	public ReadHDFSTransformer(Properties p) {
		super(p);
	}

	@Override
	public Dataset<Row> read(SparkSession session) throws Exception
	{
		
		Dataset<Row> dataframe = null;
		
		String format = props.getProperty("format").trim();
		
		switch(format) {
		case "parquet":
			dataframe = readParquet(session);
			break;
		case "text":
			dataframe = readText(session);
			break;
		case "json":
			dataframe = readJson(session);
			break;
		default:
		}
		return dataframe;
	}
	
	private Dataset<Row> readParquet( SparkSession session) 
	{
		String rawPath = props.getProperty("path");
		String inputPath = PropertyUtils.fillParameters(rawPath, props) ;
		
		Dataset<Row> dataFrame = session.read().parquet(inputPath);
		return dataFrame;
	}
	
	private Dataset<Row> readJson( SparkSession session)
	{
		String rawPath = props.getProperty("path");
		String inputPath = PropertyUtils.fillParameters(rawPath, props) ;
		
		Dataset<Row> dataFrame = session.read().json(inputPath);
		return dataFrame;		
	}

	private Dataset<Row> readText(SparkSession session) throws Exception
	{
		String[] schemaFields = getTextSchema().split(Pattern.quote(","));
		List<StructField> fields = new ArrayList<StructField>();
		final List<DataType> fieldsType = new ArrayList<DataType>();
		
		for( String schemaField : schemaFields ) {
			String fieldName = schemaField.substring(0, schemaField.indexOf(":")).trim();
			String fieldType = schemaField.substring(schemaField.indexOf(":")+1).trim();
			
			DataType type = null;
			switch( fieldType.toLowerCase() ) {
			case "string":
				type = DataTypes.StringType;
				break;
			case "int":
				type = DataTypes.IntegerType;
				break;
			case "long":
				type = DataTypes.LongType;
				break;
			case "float":
				type = DataTypes.FloatType;
				break;
			case "double":
				type = DataTypes.DoubleType;
				break;
			default:
				throw new RuntimeException("Unsupported data type: " + fieldType + " for field " + fieldName);
			}
			
			fields.add(DataTypes.createStructField(fieldName, type, true));
			fieldsType.add(type);
		}
		
		final boolean addFilename = Boolean.parseBoolean(props.getProperty("append.filename", "false"));
		if( addFilename ) {
			fields.add(DataTypes.createStructField("filename", DataTypes.StringType, true));
		}
		
		StructType schema = DataTypes.createStructType(fields);
		
		String rawPath = props.getProperty("path");
		String inputPath = PropertyUtils.fillParameters(rawPath, props) ;
		
		List<String> paths = new ArrayList<String>();
		if( addFilename ) {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] statuses = fs.listStatus(new Path(inputPath));
			for( FileStatus status : statuses ) {
				String filename = status.getPath().toString();
				paths.add(filename);
			}
		} else {
			paths.add(inputPath);
		}
		
		final String delimitor = props.getProperty("delimiter");
		
		JavaRDD<Row> rowRecords = null;
		
		for( final String path : paths ) {
		
			JavaRDD<String> pathTextRecords = session.sparkContext().textFile(path, 0).toJavaRDD();
			JavaRDD<Row> pathRowRecords = pathTextRecords.map( new Function<String, Row>(){

				@Override
				public Row call(String record) throws Exception {
					String[] fields = record.split(Pattern.quote(resolveDelimitor(delimitor)), -1);
						
					List<Object> listObjs = new ArrayList();
					for( int fieldNum = 0; fieldNum < fields.length; fieldNum++) {
						String fieldString = fields[fieldNum];
						String fieldTypeString = fieldsType.get(fieldNum).typeName().toLowerCase();
						
						if(!fieldTypeString.equals("string") && fieldString.equals("")) {
							listObjs.add(null);
							continue;
						}
						
						switch(fieldTypeString) {
						case "string":
							listObjs.add(fieldString);
							break;
						case "integer":
							listObjs.add(Integer.parseInt(fieldString));
							break;
						case "fload":
							listObjs.add(Float.parseFloat(fieldString));
							break;
						case "long":
							listObjs.add(Long.parseLong(fieldString));
							break;
						case "double":
							listObjs.add(Double.parseDouble(fieldString));
							break;
						default:
							throw new RuntimeException("Unsupported data type: " + fieldTypeString);
						}
					}
					
					if( addFilename ) {
						listObjs.add(path);
					}
					return RowFactory.create(listObjs.toArray());
				}
				
			});
			
			if( rowRecords == null ) {
				rowRecords = pathRowRecords;
			} else {
				rowRecords = rowRecords.union(pathRowRecords);
			}
			
		}
		
		return session.createDataFrame(rowRecords, schema);
	}
	
	private String getTextSchema() throws Exception
	{
		String schema = null;
		
		if( props.containsKey("schema.literal")) {
			schema = props.getProperty("schema.literal");
		} else if( props.containsKey("schema.file")) {
			String path = (String) props.get("schema.file");
			String rawSchema = HDFSUtils.hdfsFileAsString(path);
			rawSchema = rawSchema.replaceAll("\\n", ",");
		} else {
			throw new RuntimeException("unsuported schema type");
		}
		
		return schema;
	}
	
	private String resolveDelimitor(String delimitor)
	{
		if( delimitor.startsWith("chars:")) {
			String[] codePoints = delimitor.substring("chars:".length()).split(",");
			
			StringBuilder dels = new StringBuilder();
			for( String p : codePoints ) {
				dels.append(Character.toChars(Integer.parseInt(p)));
			}
			
			return dels.toString();
		} else {
			return delimitor;
		}
	}
}
