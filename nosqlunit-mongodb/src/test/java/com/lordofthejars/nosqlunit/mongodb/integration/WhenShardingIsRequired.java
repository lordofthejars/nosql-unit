package com.lordofthejars.nosqlunit.mongodb.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.mongodb.shard.ManagedMongosLifecycleManagerBuilder.newManagedMongosLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.shard.ShardedGroupBuilder.shardedGroup;

import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.shard.ShardedManagedMongoDb;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class WhenShardingIsRequired {

	static {
		System.setProperty("MONGO_HOME", "/opt/mongo");
	}

	@ClassRule
	public static ShardedManagedMongoDb shardedManagedMongoDb = shardedGroup()
																	.shard(newManagedMongoDbLifecycle().port(27018).dbRelativePath("rs-1").logRelativePath("log-1").get())
																	.shard(newManagedMongoDbLifecycle().port(27019).dbRelativePath("rs-2").logRelativePath("log-2").get())
																	.config(newManagedMongoDbLifecycle().port(27020).dbRelativePath("rs-3").logRelativePath("log-3").get())
																	.mongos(newManagedMongosLifecycle().configServer(27020).get())
																	.get();

	@AfterClass
	public static void tearDown() {
		System.clearProperty("MONGO_HOME");
	}
	
	@Test
	public void two_shards_scenario_should_be_started() throws UnknownHostException {
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		CommandResult stats = mongoClient.getDB("admin").getStats();
		
		DBObject configServer = (DBObject)stats.get("raw");
		assertThat(configServer.containsField("localhost:27020"), is(true));		
		
	}
	
}
