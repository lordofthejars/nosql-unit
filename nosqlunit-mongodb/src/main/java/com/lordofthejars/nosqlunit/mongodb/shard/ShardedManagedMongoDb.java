package com.lordofthejars.nosqlunit.mongodb.shard;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.mongodb.MongoClient;

public class ShardedManagedMongoDb extends ExternalResource {

	private static final String HOST_PORT_SEPARATOR = ":";

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ShardedManagedMongoDb.class);

	private ShardedGroup shardedGroup;

	protected ShardedManagedMongoDb(ShardedGroup shardedGroup) {
		this.shardedGroup = shardedGroup;

	}

	public void shutdownServer(int port) {

		AbstractLifecycleManager stoppingServer = shardedGroup
				.getStoppingServer(port);

		if (stoppingServer != null) {
			stoppingServer.stopEngine();
		}

	}

	public void startupServer(int port) throws Throwable {

		AbstractLifecycleManager startingServer = shardedGroup
				.getStartingServer(port);

		if (startingServer != null) {
			startingServer.startEngine();
		}

	}

	private boolean isServerStarted(
			AbstractLifecycleManager abstractLifecycleManager) {
		return abstractLifecycleManager.isReady();
	}

	private boolean isServerStopped(
			AbstractLifecycleManager abstractLifecycleManager) {
		return !abstractLifecycleManager.isReady();
	}

	@Override
	protected void before() throws Throwable {

		wakeUpShards();
		wakeUpConfigs();
		wakeUpMongos();
		registerAllShards();
	}

	private Set<String> shardsUri() {
		
		List<ManagedMongoDbLifecycleManager> shards = shardedGroup.getShards();
		
		Set<String> shardsUri = new HashSet<String>();
		
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : shards) {
			shardsUri.add(managedMongoDbLifecycleManager.getHost()+HOST_PORT_SEPARATOR+Integer.toString(managedMongoDbLifecycleManager.getPort()));
		}
		
		return shardsUri;
		
	}
	
	private void wakeUpMongos() throws Throwable {
		
		LOGGER.info("Starting Mongos");
		
		List<ManagedMongosLifecycleManager> mongos = shardedGroup.getMongos();

		for (ManagedMongosLifecycleManager managedMongosLifecycleManager : mongos) {
			if (isServerStopped(managedMongosLifecycleManager)) {
				managedMongosLifecycleManager.startEngine();
			}
		}
		
		LOGGER.info("Started Mongos");
		
	}

	private void wakeUpConfigs() throws Throwable {
		
		LOGGER.info("Starting Configs");
		
		List<ManagedMongoDbLifecycleManager> configs = shardedGroup
				.getConfigs();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : configs) {
			if (isServerStopped(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.startEngine();
			}
		}
		
		LOGGER.info("Started Configs");
		
	}

	private void wakeUpShards() throws Throwable {
		
		LOGGER.info("Starting Shards");
		
		List<ManagedMongoDbLifecycleManager> shards = shardedGroup.getShards();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : shards) {
			if (isServerStopped(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.startEngine();
			}
		}
		
		LOGGER.info("Started Shards");
	}

	private void registerAllShards() throws UnknownHostException {
		MongoClient mongosMongoClient = getMongosMongoClient();
		
		if(shardedGroup.isAuthenticationSet()) {
			MongoDbCommands.addShard(mongosMongoClient, shardsUri(), shardedGroup.getUsername(), shardedGroup.getPassword());
		} else {
			MongoDbCommands.addShard(mongosMongoClient, shardsUri());
		}
		
		mongosMongoClient.close();
	}

	private MongoClient getMongosMongoClient() throws UnknownHostException {
		
		ManagedMongosLifecycleManager firstMongosServer = shardedGroup.getFirstMongosServer();
		MongoClient mongoClient = new MongoClient(firstMongosServer.getHost(), firstMongosServer.getPort());
		
		return mongoClient;
		
	}
	
	@Override
	protected void after() {
		
		shutdownMongos();
		shutdownConfigs();
		shutdownShards();
		
	}

	private void shutdownMongos() {
		
		LOGGER.info("Stopping Mongos");
		
		List<ManagedMongosLifecycleManager> mongos = shardedGroup.getMongos();

		for (ManagedMongosLifecycleManager managedMongosLifecycleManager : mongos) {
			if (isServerStarted(managedMongosLifecycleManager)) {
				managedMongosLifecycleManager.stopEngine();
			}
		}
		
		LOGGER.info("Stopped Mongos");
		
	}

	private void shutdownConfigs() {
		
		LOGGER.info("Stopping Configs");
		
		List<ManagedMongoDbLifecycleManager> configs = shardedGroup
				.getConfigs();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : configs) {
			if (isServerStarted(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.stopEngine();
			}
		}
		
		LOGGER.info("Stopped Configs");
		
	}

	private void shutdownShards() {
		
		LOGGER.info("Stopping Shards");
		
		List<ManagedMongoDbLifecycleManager> shards = shardedGroup.getShards();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : shards) {
			if (isServerStarted(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.stopEngine();
			}
		}
		
		LOGGER.info("Stopped Shards");
	}
	
}
