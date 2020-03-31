package com.andrew.sparkwork;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.andrew.sparkwork.utils.PropertyUtils;

public class SparkRunner {
	
	private static final Logger log = LoggerFactory.getLogger(SparkRunner.class);

	public static void main(String[] args)
	{
		SparkContext sc = null;
		
		int exitCode = 0;
		
		try{
			Properties props = PropertyUtils.loadPropertiesFromArgs(args);
			
			SparkConf conf = new SparkConf();
			
			conf.set("spark.sql.parquet.binaryAsString", "true");
			
			sc = new SparkContext(conf);
			
			SparkSession session = SparkSession.builder()
					.config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport()
					.getOrCreate();
		
			System.out.println("Beign to kick off the process");
			SparkRunnerImpl.SparkRun(session, props);
			System.out.println("Stop the process");
		}
		catch(Exception e ) {
			log.error(e.getMessage(), e);
			System.out.println( e.getMessage() );
			e.printStackTrace();
			exitCode = -200;
		} finally {
			if( sc != null ) {
				sc.stop();
			}
			System.exit(exitCode);
		}
		
	}
	
}
