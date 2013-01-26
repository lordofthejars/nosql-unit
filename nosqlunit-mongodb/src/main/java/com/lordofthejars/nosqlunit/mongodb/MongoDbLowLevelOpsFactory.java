package com.lordofthejars.nosqlunit.mongodb;

public class MongoDbLowLevelOpsFactory {

	private static MongoDbLowLevelOps mongoDbLowLevelOps = null;
	
	public static final MongoDbLowLevelOps getSingletonInstance() {
		if(mongoDbLowLevelOps == null) {
			mongoDbLowLevelOps = new MongoDbLowLevelOps();
		}
		
		return mongoDbLowLevelOps;
	}
	
}
