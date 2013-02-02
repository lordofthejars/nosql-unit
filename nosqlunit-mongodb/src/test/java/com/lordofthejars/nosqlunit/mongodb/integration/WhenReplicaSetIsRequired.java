package com.lordofthejars.nosqlunit.mongodb.integration;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;

import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetManagedMongoDb;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class WhenReplicaSetIsRequired {

	static {
		System.setProperty("MONGO_HOME", "/opt/mongo");
	}
	
	@ClassRule
	public static ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs-test")
																		.eligible(
																					newManagedMongoDbLifecycle().port(27017).dbRelativePath("rs-0").logRelativePath("log-0").get()
																				 )
																		.eligible(
																					newManagedMongoDbLifecycle().port(27018).dbRelativePath("rs-1").logRelativePath("log-1").get()
																				 )
																		.eligible(
																					newManagedMongoDbLifecycle().port(27019).dbRelativePath("rs-2").logRelativePath("log-2").get()
																				 )
																	  .get();
	
	@Test
	public void three_member_set_scenario_should_be_started() throws UnknownHostException, InterruptedException {
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		DBObject replicaSetGetStatus = MongoDbCommands.replicaSetGetStatus(mongoClient);
		assertThat(countPrimary(replicaSetGetStatus), is(1));
		assertThat(countSecondaries(replicaSetGetStatus), is(2));
		
		mongoClient.close();
		
	}

	@Test
	public void server_should_be_able_to_stopped_programmatically() throws UnknownHostException {
		
		replicaSetManagedMongoDb.shutdownServer(27017);
		replicaSetManagedMongoDb.waitUntilReplicaSetBecomesStable();
		
		MongoClient mongoClient = new MongoClient("localhost", 27018);
		DBObject replicaSetGetStatus = MongoDbCommands.replicaSetGetStatus(mongoClient);
		
		assertThat(countPrimary(replicaSetGetStatus), is(1));
		assertThat(countSecondaries(replicaSetGetStatus), is(1));
		
		mongoClient.close();
		
	}
	
	private int countSecondaries(DBObject configuration) {
		return countStates(configuration, "SECONDARY");
	}
	
	private int countPrimary(DBObject configuration) {
		return countStates(configuration, "PRIMARY");
	}

	private int countStates(DBObject configuration, String wantedState) {
		int number = 0;
		
		BasicDBList basicDBList = (BasicDBList) configuration.get("members");
		
		for (Object object : basicDBList) {
			
			DBObject server = (DBObject)object;
			String state = (String) server.get("stateStr");
			
			if(state.equalsIgnoreCase(wantedState)) {
				number++;
			}
			
		}
		return number;
	}
	
}
