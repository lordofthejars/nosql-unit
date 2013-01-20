package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.mongodb.replicaset.ConfigurationDocument;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBCommands {

	private static final String REPL_SET_GET_STATUS_COMMAND = "replSetGetStatus";
	private static final String REPL_SET_INITIATE_COMMAND = "replSetInitiate";
	private static final String RECONFIG_COMMAND = "replSetReconfig";	
	
	private MongoDBCommands() {
		super();
	}
	
	public static DBObject replicaSetGetStatus(MongoClient mongoClient) {
		return mongoClient.getDB("admin").command(new BasicDBObject(REPL_SET_GET_STATUS_COMMAND, 1));
	}
	
	public static DBObject replicaSetGetStatus(MongoClient mongoClient, String username, String password) {
		DB adminDb = mongoClient.getDB("admin");
		adminDb.authenticate(username, password.toCharArray());
		return adminDb.command(new BasicDBObject(REPL_SET_GET_STATUS_COMMAND, 1));
	}
	
	public static CommandResult replicaSetInitiate(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
		DB adminDb = mongoClient.getDB("admin");
		BasicDBObject command = new BasicDBObject(REPL_SET_INITIATE_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}
	
	public static CommandResult replicaSetInitiate(MongoClient mongoClient, ConfigurationDocument configurationDocument, String username, String password) {
		DB adminDb = mongoClient.getDB("admin");
		adminDb.authenticate(username, password.toCharArray());
		BasicDBObject command = new BasicDBObject(REPL_SET_INITIATE_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}
	
	public static CommandResult replSetReconfig(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
		DB adminDb = mongoClient.getDB("admin");
		BasicDBObject command = new BasicDBObject(RECONFIG_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}
	
	public static CommandResult replSetReconfig(MongoClient mongoClient, ConfigurationDocument configurationDocument, String username, String password) {
		DB adminDb = mongoClient.getDB("admin");
		adminDb.authenticate(username, password.toCharArray());
		BasicDBObject command = new BasicDBObject(RECONFIG_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}
}
