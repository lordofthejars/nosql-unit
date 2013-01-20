package com.lordofthejars.nosqlunit.mongodb.replicaset;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ReplicaSetManagedMongoDb extends ExternalResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSetManagedMongoDb.class); 
	
	private static final String REPL_SET_INITIATE_COMMAND = "replSetInitiate";
	
	private ReplicaSetGroup replicaSetGroup;
	
	protected ReplicaSetManagedMongoDb(ReplicaSetGroup replicaSetGroup) {
		this.replicaSetGroup = replicaSetGroup;
	}
	
	
	@Override
	protected void before() throws Throwable {
		wakeUpServers();
		replicaSetInitiate();
	}


	@Override
	protected void after() {
		shutdownServers();
	}

	protected List<ManagedMongoDbLifecycleManager> getServers() {
		return replicaSetGroup.getServers();
	}
	
	protected ConfigurationDocument getConfigurationDocument() {
		return replicaSetGroup.getConfiguration();
	}

	private void replicaSetInitiate() throws UnknownHostException {
		BasicDBObject cmd = new BasicDBObject(REPL_SET_INITIATE_COMMAND, getConfigurationDocument().getConfiguration());
		CommandResult commandResult = runCommandToAdmin(cmd);
		
		LOGGER.info("Command {} has returned {}", REPL_SET_INITIATE_COMMAND, commandResult.toString());
		
	}
	
	private CommandResult runCommandToAdmin(DBObject cmd) throws UnknownHostException {
		MongoClient mongoClient = getMongoClient();
		return mongoClient.getDB("admin").command(cmd);
	}
	
	private void shutdownServers() {
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup.getServers()) {
			managedMongoDbLifecycleManager.stopEngine();
		}
	}
	
	private void wakeUpServers() throws Throwable {
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup.getServers()) {
			managedMongoDbLifecycleManager.startEngine();
		}
	}
	
	private MongoClient getMongoClient() throws UnknownHostException {
		
		ManagedMongoDbLifecycleManager defaultConnection = replicaSetGroup.getDefaultConnection();
		return new MongoClient(defaultConnection.getHost(), defaultConnection.getPort());
		
	}
	
}
