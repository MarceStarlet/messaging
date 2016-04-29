package com.marcestarlet.messaging.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author MarceStarlet
 * Load the Properties file for the broker configuration
 */
public class BrokerConfiguration {
	
	private static BrokerConfiguration configuration = new BrokerConfiguration();
	private Properties properties;

	private BrokerConfiguration(){	
		// load the properties when the instance is created
		properties = getProperties();
	}
	
	/**
	 * Get a single instance
	 * @return BrokerConfiguration
	 */
	public static BrokerConfiguration getInstance(){
		return configuration;
	}
	
	/**
	 * Loads properties file
	 * @return Properties
	 */
	private Properties getProperties(){
		Properties prop = new Properties();
		
		try(InputStream in = new FileInputStream("config.properties")){
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return prop;
	}
	
	/**
	 * Get a property if exists returns the property value
	 * otherwise returns the default value parameter
	 * @param property
	 * @param value
	 * @return String value
	 */
	public String getProperty(String property, String value){
		return properties.getProperty(property, value);
	}
	
}
