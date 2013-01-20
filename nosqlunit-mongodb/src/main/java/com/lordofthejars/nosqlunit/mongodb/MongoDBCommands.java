package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBCommands {

	private MongoDBCommands() {
		super();
	}
	
	public static DBObject replicaSetGetStatus(MongoClient mongoClient) {
		return mongoClient.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));
	}
	
}
