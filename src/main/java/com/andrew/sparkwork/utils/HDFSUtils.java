package com.andrew.sparkwork.utils;

import java.io.InputStreamReader;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.spark_project.guava.io.CharStreams;

public class HDFSUtils {

	public static String hdfsFileAsString(String hdfsFile) throws Exception
	{
		String contents = null;
		
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream stream = fs.open(new Path(hdfsFile));
		
		contents = CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8));
		stream.close();
		
		return contents;
	}
	
	public static void removeHDFSPath(String hdfsFile) throws Exception
	{
		FileSystem.newInstance(new Configuration()).delete(new Path(hdfsFile), true);
	}
	
}
