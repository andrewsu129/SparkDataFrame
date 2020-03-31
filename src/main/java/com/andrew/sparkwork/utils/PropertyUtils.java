package com.andrew.sparkwork.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class PropertyUtils {
	
	public static Properties loadPropertiesFromArgs(String[] args) throws Exception
	{
		String[] filePaths = args[0].split(Pattern.quote(","));
		
		Properties props = new Properties();
		
		for( String localPath : filePaths ) {
			loadPropertiesFromFile( props, localPath);
		}
		
		if( args.length > 1 ) {
			String[] overrides = Arrays.copyOfRange(args, 1, args.length);
			
			for( String override : overrides) {
				String[] components = override.split(Pattern.quote("="));
				String propertyName = components[0];
				String propertyValue = components[1];
				
				props.setProperty(propertyName, propertyValue);
			}
		}
		return props;
	}

	public static void loadPropertiesFromFile(Properties props, String name) throws Exception
	{
		FileInputStream input = null;

		try{
			input = new FileInputStream(name);
			
			props.load( input);
			
		} catch (Exception e) {
			throw e;
		} finally {
			try{
				if( input != null ) {
					input.close();
				}
			} catch (IOException ioe) {
				throw ioe;
			}
		}
	}
	
	public static Properties propertiesForPrefix(Properties props, String prefix)
	{
		if( !prefix.endsWith("." )) {
			prefix += ".";
		}
		
		Properties prefProps = new Properties();
		
		for( String key : props.stringPropertyNames() ) {
			
			if( key.startsWith(prefix) ) {
				String keyWithoutPrefix = key.substring(prefix.length());
				prefProps.setProperty(keyWithoutPrefix, props.getProperty(key));
			} else {
				prefProps.setProperty(key, props.getProperty(key));
			}
			
		}
		
		return prefProps;
	}
	
	public static String fillParameters(String value, Properties props) 
	{
		Properties parameterProperties = PropertyUtils.propertiesForPrefix(props, "parameter.");
		
		System.out.println( "Parametr >>>>>>>>>>>>>>>>>>> parameterProperties " + parameterProperties.toString() );
		for( String parameter : parameterProperties.stringPropertyNames() ) {
			String propertyEnclosed = "${" + parameter + "}";
			String propertyValue = parameterProperties.getProperty(parameter);
			
			value = value.replace(propertyEnclosed, propertyValue);
		}
		
		return value;
	}
}
