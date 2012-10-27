package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class HBaseConfiguration extends AbstractJsr330Configuration {

	private Configuration configuration;
	
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public Configuration getConfiguration() {
		return configuration;
	}
	
}
