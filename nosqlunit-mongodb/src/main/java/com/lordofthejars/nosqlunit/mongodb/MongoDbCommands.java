package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.mongodb.replicaset.ConfigurationDocument;
import com.mongodb.*;

import java.util.Set;

public class MongoDbCommands {

	private static final String REPL_SET_GET_STATUS_COMMAND = "replSetGetStatus";
	private static final String REPL_SET_INITIATE_COMMAND = "replSetInitiate";
	private static final String RECONFIG_COMMAND = "replSetReconfig";	
	private static final String ADD_SHARD_COMMAND = "addshard";
	private static final String ENABLE_SHARDING_COMMAND = "enablesharding";
	private static final String SHARD_COLLECTION_COMMAND = "shardcollection";
	private static final String LIST_SHARDS_COMMAND = "listShards";
	
	private MongoDbCommands() {
		super();
	}
	
	
	public static DBObject replicaSetGetStatus(Mongo mongoClient) {
		return mongoClient.getDB("admin").command(new BasicDBObject(REPL_SET_GET_STATUS_COMMAND, 1));
	}
	
	public static CommandResult shardCollection(Mongo mongoClient, String collectionWithDatabase, DBObject shardKey) {
		DB adminDb = getAdminDatabase(mongoClient);
		BasicDBObject basicDBObject = new BasicDBObject(SHARD_COLLECTION_COMMAND, collectionWithDatabase);
		basicDBObject.put("key", shardKey);
		
		return adminDb.command(basicDBObject);
	}
	
	public static CommandResult enableSharding(MongoClient mongoClient, String database) {
		DB adminDb = getAdminDatabase(mongoClient);
		return adminDb.command(new BasicDBObject(ENABLE_SHARDING_COMMAND, database));
	}
	
	public static CommandResult listShards(MongoClient mongoClient) {
		DB adminDb = getAdminDatabase(mongoClient);
		return adminDb.command(new BasicDBObject(LIST_SHARDS_COMMAND, 1));
	}

	public static void addShard(MongoClient mongoClient, Set<String> shards) {
		DB adminDb = getAdminDatabase(mongoClient);
		
		for (String shardUri : shards) {
			adminDb.command(new BasicDBObject(ADD_SHARD_COMMAND, shardUri));			
		}
	}
	
	public static CommandResult replicaSetInitiate(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
		DB adminDb = getAdminDatabase(mongoClient);
		BasicDBObject command = new BasicDBObject(REPL_SET_INITIATE_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}


	private static DB getAdminDatabase(Mongo mongoClient) {
		DB adminDb = mongoClient.getDB("admin");
		return adminDb;
	}
	
	public static CommandResult replSetReconfig(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
		DB adminDb = getAdminDatabase(mongoClient);
		BasicDBObject command = new BasicDBObject(RECONFIG_COMMAND,
				configurationDocument.getConfiguration());
		return adminDb.command(command);
	}

	
	public static void shutdown(String host, int port) {
		MongoClient mongo = null;
		try {
			mongo = new MongoClient(host, port);
			DB db = mongo.getDB("admin");
			CommandResult shutdownResult = db.command(new BasicDBObject(
					"shutdown", 1));
			shutdownResult.throwOnError();
		} catch (MongoException e) {
			//It is ok because response could not be returned because network connection is closed.
		} catch (Throwable e) {
			throw new IllegalStateException("Mongodb could not be shutdown.", e);
		} finally {
			mongo.close();
		}
	}
}
