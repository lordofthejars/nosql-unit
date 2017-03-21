package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;
import com.mongodb.MongoClient;

public class EmbeddedMongoInstancesFactory {

	private static EmbeddedInstances<MongoClient> embeddedInstances;
	
	private EmbeddedMongoInstancesFactory() {
		super();
	}
	
	public synchronized static EmbeddedInstances<MongoClient> getInstance() {
		if(embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<>();
		}
		
		return embeddedInstances;
	}
	
}
