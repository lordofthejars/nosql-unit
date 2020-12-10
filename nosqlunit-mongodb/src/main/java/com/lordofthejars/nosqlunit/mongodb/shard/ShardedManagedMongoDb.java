package com.lordofthejars.nosqlunit.mongodb.shard;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetManagedMongoDb;
import com.mongodb.client.MongoClient;

public class ShardedManagedMongoDb extends ExternalResource {

	private static final String HOST_PORT_SEPARATOR = ":";

	private static final Logger LOGGER = LoggerFactory.getLogger(ShardedManagedMongoDb.class);

	private ShardedGroup shardedGroup;

	protected ShardedManagedMongoDb(ShardedGroup shardedGroup) {
		this.shardedGroup = shardedGroup;

	}

	public void shutdownServer(int port) {

		AbstractLifecycleManager stoppingServer = shardedGroup.getStoppingServer(port);

		if (stoppingServer != null) {
			stoppingServer.stopEngine();
		}

	}

	public void startupServer(int port) throws Throwable {

		AbstractLifecycleManager startingServer = shardedGroup.getStartingServer(port);

		if (startingServer != null) {
			startingServer.startEngine();
		}

	}

	private boolean isServerStarted(AbstractLifecycleManager abstractLifecycleManager) {
		return abstractLifecycleManager.isReady();
	}

	private boolean isServerStopped(AbstractLifecycleManager abstractLifecycleManager) {
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
			shardsUri.add(getUri(managedMongoDbLifecycleManager));
		}

		return shardsUri;

	}

	private String getUri(ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return managedMongoDbLifecycleManager.getHost() + HOST_PORT_SEPARATOR
				+ Integer.toString(managedMongoDbLifecycleManager.getPort());
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

		List<ManagedMongoDbLifecycleManager> configs = shardedGroup.getConfigs();

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : configs) {
			if (isServerStopped(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.startEngine();
			}
		}

		LOGGER.info("Started Configs");

	}

	private void wakeUpShards() throws Throwable {

		if (this.shardedGroup.isShardsAndReplicSetShardsMixed()) {
			throw new IllegalArgumentException("Cannot mix shards servers with replica set shards servers.");
		}
		
		if (this.shardedGroup.isOnlyShards()) {
			wakeUpShardsServers();
		} else {
			if(this.shardedGroup.isOnlyReplicaSetShards()) {
				wakeUpReplicaSetShardsServers();
			}
		}

	}

	private void wakeUpReplicaSetShardsServers() throws Throwable {
		LOGGER.info("Starting ReplicaSet Shards");
		
		List<ReplicaSetManagedMongoDb> replicaSets = shardedGroup.getReplicaSets();
		
		for (ReplicaSetManagedMongoDb replicaSetManagedMongoDb : replicaSets) {
			replicaSetManagedMongoDb.startAllReplicaSet();
		}
		
		LOGGER.info("Started ReplicaSet Shards");
	}

	private void wakeUpShardsServers() throws Throwable {
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

		if (this.shardedGroup.isShardsAndReplicSetShardsMixed()) {
			throw new IllegalArgumentException("Cannot mix shards servers with replica set shards servers.");
		}

		MongoClient mongosMongoClient = null;
		try {
			mongosMongoClient = getMongosMongoClient();

			if (this.shardedGroup.isOnlyShards()) {
				registerShardServers(mongosMongoClient);
			} else {
				if (this.shardedGroup.isOnlyReplicaSetShards()) {
					registerReplicaSetShardServers(mongosMongoClient);
				}
			}
		} finally {
			if (mongosMongoClient != null) {
				mongosMongoClient.close();
			}
		}
	}

	private void registerReplicaSetShardServers(MongoClient mongosMongoClient) {
		Set<String> replicaSetShardsConfig = buildReplicaSetShardAddingCommand();

		MongoDbCommands.addShard(mongosMongoClient, replicaSetShardsConfig);
	}

	private Set<String> buildReplicaSetShardAddingCommand() {
		Set<String> replicaSetShardsConfig = new HashSet<String>();

		for (ReplicaSetManagedMongoDb replicaSetManagedMongoDb : this.shardedGroup.getReplicaSets()) {
			List<ManagedMongoDbLifecycleManager> replicaSetServers = replicaSetManagedMongoDb.getReplicaSetServers();
			replicaSetShardsConfig.add(shardUri(replicaSetManagedMongoDb.replicaSetName(), replicaSetServers));
		}

		return replicaSetShardsConfig;
	}

	private void registerShardServers(MongoClient mongosMongoClient) {
		MongoDbCommands.addShard(mongosMongoClient, shardsUri());
	}

	private String shardUri(String replicaSetName, List<ManagedMongoDbLifecycleManager> managedMongoDbLifecycleManagers) {

		StringBuilder stringBuilder = new StringBuilder(replicaSetName);
		stringBuilder.append("/");

		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : managedMongoDbLifecycleManagers) {
			stringBuilder.append(getUri(managedMongoDbLifecycleManager));
			stringBuilder.append(", ");
		}

		return stringBuilder.toString().substring(0, stringBuilder.length() - 2);
	}

	private MongoClient getMongosMongoClient() throws UnknownHostException {

		ManagedMongosLifecycleManager firstMongosServer = shardedGroup.getFirstMongosServer();
		if(shardedGroup.isAuthenticationSet()) {
			MongoCredential credential = MongoCredential.createCredential(this.shardedGroup.getUsername(),
					"admin",
					this.shardedGroup.getPassword().toCharArray());

			return MongoClients.create(
					MongoClientSettings.builder()
							.credential(credential)
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(
											new ServerAddress(firstMongosServer.getHost(), firstMongosServer.getPort())
											)))
							.build());
		} else {


			return MongoClients.create(
					MongoClientSettings.builder()
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(
											new ServerAddress(firstMongosServer.getHost(), firstMongosServer.getPort())
									)))
							.build());

		}

	}

	@Override
	protected void after() {

		shutdownMongos();
		shutdownConfigs();
		shutdownShards();
		shutdownReplicaSetShards();

	}

	private void shutdownReplicaSetShards() {
		
		LOGGER.info("Stopping ReplicaSet Shards");
		
		List<ReplicaSetManagedMongoDb> replicaSets = shardedGroup.getReplicaSets();
		
		for (ReplicaSetManagedMongoDb replicaSetManagedMongoDb : replicaSets) {
			replicaSetManagedMongoDb.stopAllReplicaSet();
		}
		
		LOGGER.info("Stopped ReplicaSet Shards");
		
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

		List<ManagedMongoDbLifecycleManager> configs = shardedGroup.getConfigs();

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
