package com.lordofthejars.nosqlunit.mongodb.replicaset;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.lordofthejars.nosqlunit.mongodb.MongoDbLowLevelOps;
import com.lordofthejars.nosqlunit.mongodb.MongoDbLowLevelOpsFactory;
import com.mongodb.CommandResult;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class ReplicaSetManagedMongoDb extends ExternalResource {


	private static final Logger LOGGER = LoggerFactory
			.getLogger(ReplicaSetManagedMongoDb.class);


	private ReplicaSetGroup replicaSetGroup;

	private MongoDbLowLevelOps mongoDbLowLevelOps = MongoDbLowLevelOpsFactory.getSingletonInstance();
	
	protected ReplicaSetManagedMongoDb(ReplicaSetGroup replicaSetGroup) {
		this.replicaSetGroup = replicaSetGroup;
	}

	public String replicaSetName() {
		return this.replicaSetGroup.getReplicaSetName();
	}
	
	public List<ManagedMongoDbLifecycleManager> getReplicaSetServers() {
		return this.replicaSetGroup.getServers();
	}
	
	public void shutdownServer(int port) {

		ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager = replicaSetGroup
				.getStartedServer(port);

		if (managedMongoDbLifecycleManager != null) {
			managedMongoDbLifecycleManager.stopEngine();
		}
		
	}

	public void waitUntilReplicaSetBecomesStable() {
		MongoClient mongoClient;
		try {
			mongoClient = getAvailableServersMongoClient();
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}

		waitingToBecomeStable(mongoClient);

		mongoClient.close();
		
	}

	private void waitingToBecomeStable(MongoClient mongoClient) {
		if(replicaSetGroup.isAuthenticationSet()) {
			this.mongoDbLowLevelOps.waitUntilReplicaSetBecomeStable(mongoClient, this.replicaSetGroup.numberOfStartedServers(), replicaSetGroup.getUsername(), replicaSetGroup.getPassword());
		} else {
			this.mongoDbLowLevelOps.waitUntilReplicaSetBecomeStable(mongoClient, this.replicaSetGroup.numberOfStartedServers());
		}
	}

	public void startupServer(int port) throws Throwable {

		ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager = replicaSetGroup.getStoppedServer(port);

		if (managedMongoDbLifecycleManager != null) {
			managedMongoDbLifecycleManager.startEngine();
		}

	}

	public ManagedMongoDbLifecycleManager getServerByPortAndState(int port, boolean state) {
		if(state) {
			return replicaSetGroup.getStartedServer(port);			
		} else {
			return replicaSetGroup.getStoppedServer(port);
		}
	}
	
	@Override
	protected void before() throws Throwable {
		wakeUpServers();
		replicaSetInitiate();
		waitUntilConfigurationSpreadAcrossServersFromDefaultConnection();
	}

	public void startAllReplicaSet() throws Throwable {
		this.before();
	}
	
	private void waitUntilConfigurationSpreadAcrossServersFromDefaultConnection() throws UnknownHostException {
		
		MongoClient mongoClient = getDefaultMongoClient();
		
		waitingToBecomeStable(mongoClient);
		
		mongoClient.close();

	}

	@Override
	protected void after() {
		shutdownServers();
	}

	public void stopAllReplicaSet() {
		this.after();
	}
	
	protected List<ManagedMongoDbLifecycleManager> getServers() {
		return replicaSetGroup.getServers();
	}

	protected ConfigurationDocument getConfigurationDocument() {
		return replicaSetGroup.getConfiguration();
	}

	private void replicaSetInitiate() throws UnknownHostException {

		CommandResult commandResult = runCommandToAdmin(getConfigurationDocument());

		LOGGER.info("Command {} has returned {}", "replSetInitiaite",
				commandResult.toString());

	}

	private CommandResult runCommandToAdmin(ConfigurationDocument cmd)
			throws UnknownHostException {
		
		MongoClient mongoClient = getDefaultMongoClient();

		CommandResult commandResult = null;
		if (this.replicaSetGroup.isAuthenticationSet()) {
			commandResult = MongoDbCommands.replicaSetInitiate(mongoClient, cmd,
					this.replicaSetGroup.getUsername(),
					this.replicaSetGroup.getPassword());
			
		} else {
			commandResult = MongoDbCommands.replicaSetInitiate(mongoClient, cmd);
		}
		
		mongoClient.close();
		return commandResult;
	}

	private void shutdownServers() {
		
		LOGGER.info("Stopping Replica Set servers");
		
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup
				.getServers()) {
			if(isServerStarted(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.stopEngine();
			}
		}
		
		LOGGER.info("Stopped Replica Set servers");
	}

	private void wakeUpServers() throws Throwable {
		
		LOGGER.info("Starting Replica Set servers");
		
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup
				.getServers()) {
			if (isServerStopped(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.startEngine();
			}
		}
		
		LOGGER.info("Started Replica Set servers");
	}

	private boolean isServerStarted(ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return managedMongoDbLifecycleManager.isReady();
	}
	
	private boolean isServerStopped(
			ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return !managedMongoDbLifecycleManager.isReady();
	}

	private MongoClient getAvailableServersMongoClient()
			throws UnknownHostException {

		List<ServerAddress> seeds = new ArrayList<ServerAddress>();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup
				.getServers()) {
			if (managedMongoDbLifecycleManager.isReady()) {
				ServerAddress serverAddress = new ServerAddress(
						managedMongoDbLifecycleManager.getHost(),
						managedMongoDbLifecycleManager.getPort());
				seeds.add(serverAddress);
			}
		}

		return new MongoClient(seeds);

	}

	private MongoClient getDefaultMongoClient() throws UnknownHostException {

		ManagedMongoDbLifecycleManager defaultConnection = replicaSetGroup
				.getDefaultConnection();
		return new MongoClient(defaultConnection.getHost(),
				defaultConnection.getPort());

	}

}
