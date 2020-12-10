package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;
import com.mongodb.client.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;

public class EmbeddedMongoInstancesFactory {

	private static EmbeddedInstances<MongoClient> embeddedInstances;

	private static EmbeddedInstances<MongodExecutable> embeddedServerInstances;

	private EmbeddedMongoInstancesFactory() {
		super();
	}
	
	public synchronized static EmbeddedInstances<MongoClient> getInstance() {
		if(embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<>();
		}
		
		return embeddedInstances;
	}

	public synchronized static EmbeddedInstances<MongodExecutable> getServerInstance() {
		if(embeddedServerInstances == null) {
			embeddedServerInstances = new EmbeddedInstances<>();
		}

		return embeddedServerInstances;
	}
	
}
