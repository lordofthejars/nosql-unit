package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;
import com.mongodb.Mongo;

public class EmbeddedMongoInstancesFactory {

private static EmbeddedInstances<Mongo> embeddedInstances;
	
	private EmbeddedMongoInstancesFactory() {
		super();
	}
	
	public synchronized static EmbeddedInstances<Mongo> getInstance() {
		if(embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<Mongo>();
		}
		
		return embeddedInstances;
	}
	
}
