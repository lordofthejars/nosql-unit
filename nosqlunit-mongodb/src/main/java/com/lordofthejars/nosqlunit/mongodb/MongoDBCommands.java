package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBCommands {

	private MongoDBCommands() {
		super();
	}
	
	public static DBObject replicaSetGetStatus(MongoClient mongoClient) {
		return mongoClient.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));
	}
	
	public static DBObject replicaSetGetStatus(MongoClient mongoClient, String username, String password) {
		DB adminDb = mongoClient.getDB("admin");
		adminDb.authenticate(username, password.toCharArray());
		return adminDb.command(new BasicDBObject("replSetGetStatus", 1));
	}
	
	public static CommandResult replicaSetInitiate(MongoClient mongoClient, DBObject command) {
		DB adminDb = mongoClient.getDB("admin");
		return adminDb.command(command);
	}
	
	public static CommandResult replicaSetInitiate(MongoClient mongoClient, DBObject command, String username, String password) {
		DB adminDb = mongoClient.getDB("admin");
		adminDb.authenticate(username, password.toCharArray());
		return adminDb.command(command);
	}
}
