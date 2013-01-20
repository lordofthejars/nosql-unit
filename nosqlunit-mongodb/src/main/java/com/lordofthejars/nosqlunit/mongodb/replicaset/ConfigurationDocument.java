package com.lordofthejars.nosqlunit.mongodb.replicaset;

import com.mongodb.BasicDBObject;

public class ConfigurationDocument {

	private BasicDBObject configuration;
	
	public ConfigurationDocument(BasicDBObject configuration) {
		this.configuration = configuration;
	}
	
	public BasicDBObject getConfiguration() {
		return configuration;
	}
	
}
