package com.lordofthejars.nosqlunit.mongodb.replicaset;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.is;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;

import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.MongoDBCommands;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetManagedMongoDb;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

public class WhenReplicaSetIsRequired {

	static {
		System.setProperty("MONGO_HOME", "/opt/mongo");
	}
	
	@ClassRule
	public static ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs-test")
																		.eligible(
																					newManagedMongoDbLifecycle().port(27017).dbRelativePath("rs-0").get()
																				 )
																		.eligible(
																					newManagedMongoDbLifecycle().port(27018).dbRelativePath("rs-1").get()
																				 )
																		.eligible(
																					newManagedMongoDbLifecycle().port(27019).dbRelativePath("rs-2").get()
																				 )
																	  .get();
	
	@Test
	public void test() throws UnknownHostException, InterruptedException {
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		TimeUnit.SECONDS.sleep(1);
		
		DBObject replicaSetGetStatus = MongoDBCommands.replicaSetGetStatus(mongoClient);
		String serialize = JSON.serialize(replicaSetGetStatus);
		System.out.println(serialize);
		
	}

	
}
