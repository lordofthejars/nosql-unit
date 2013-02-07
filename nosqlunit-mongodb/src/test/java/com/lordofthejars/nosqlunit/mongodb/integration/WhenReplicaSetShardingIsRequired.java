package com.lordofthejars.nosqlunit.mongodb.integration;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static com.lordofthejars.nosqlunit.mongodb.shard.ManagedMongosLifecycleManagerBuilder.newManagedMongosLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.shard.ShardedGroupBuilder.shardedGroup;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;

import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.lordofthejars.nosqlunit.mongodb.shard.ShardedManagedMongoDb;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class WhenReplicaSetShardingIsRequired {

	static {
		System.setProperty("MONGO_HOME", "/opt/mongo");
	}

	@ClassRule
	public static ShardedManagedMongoDb shardedManagedMongoDb = shardedGroup()
																	.replicaSet(replicaSet("rs-test-1")
																		.eligible(
																					newManagedMongoDbLifecycle().port(27007).dbRelativePath("rs-0").logRelativePath("log-0").get()
																				 )
																	  .get())
																	 .replicaSet(replicaSet("rs-test-2")
																		.eligible(
																					newManagedMongoDbLifecycle().port(27009).dbRelativePath("rs-0").logRelativePath("log-0").get()
																				 )
																	  .get())
																	.config(newManagedMongoDbLifecycle().port(27020).dbRelativePath("rs-3").logRelativePath("log-3").get())
																	.mongos(newManagedMongosLifecycle().configServer(27020).get())
																	.get();
	
	@Test
	public void sharded_replica_set_should_be_started() throws UnknownHostException {
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		CommandResult listShards = MongoDbCommands.listShards(mongoClient);
		
		assertThat((String)listShards.get("serverUsed"), is("localhost/127.0.0.1:27017"));
		BasicDBList shards = (BasicDBList) listShards.get("shards");
		
		DBObject replicaSet1 = selectFirst(shards, having(on(DBObject.class).get("_id"), is("rs-test-2")));
		
		assertThat(replicaSet1, is(createShardDbObject("rs-test-2", "rs-test-2/localhost:27009")));
		
		DBObject replicaSet2 = selectFirst(shards, having(on(DBObject.class).get("_id"), is("rs-test-1")));
		
		assertThat(replicaSet2, is(createShardDbObject("rs-test-1", "rs-test-1/localhost:27007")));
		
	}
	
	private DBObject createShardDbObject(String id, String host) {
		BasicDBObjectBuilder basicDBObjectBuilder = new BasicDBObjectBuilder();
		return basicDBObjectBuilder.append("_id", id).append("host", host).get();
	}
	
}
