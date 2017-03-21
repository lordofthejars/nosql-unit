package com.lordofthejars.nosqlunit.mongodb.replicaset;

import org.bson.Document;

public class ConfigurationDocument {

	private Document configuration;
	
	public ConfigurationDocument(Document configuration) {
		this.configuration = configuration;
	}
	
	public Document getConfiguration() {
		return this.configuration;
	}
	
	public String getReplicaSetName() {
		return (String) this.configuration.get(ReplicaSetConfigurationBuilder.ID_TAG);
	}
	
}
