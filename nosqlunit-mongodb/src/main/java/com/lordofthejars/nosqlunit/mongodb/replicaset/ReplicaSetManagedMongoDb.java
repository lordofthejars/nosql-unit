package com.lordofthejars.nosqlunit.mongodb.replicaset;

import static org.hamcrest.CoreMatchers.is;
import static ch.lambdaj.Lambda.selectFirst;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.MongoDBCommands;
import com.mongodb.BasicDBList;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class ReplicaSetManagedMongoDb extends ExternalResource {

	private static final int WAIT_TIME = 2;

	private static final String ERRMSG_TOKEN = "errmsg";

	private static final String MEMBERS_TOKEN = "members";
	private static final String STATE_TOKEN = "stateStr";
	private static final String UNKNOWN_TOKEN = "UNKNOWN";

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ReplicaSetManagedMongoDb.class);

	private static final int MAX_RETRIES = 20;

	private ReplicaSetGroup replicaSetGroup;

	protected ReplicaSetManagedMongoDb(ReplicaSetGroup replicaSetGroup) {
		this.replicaSetGroup = replicaSetGroup;
	}

	public void shutdownServer(int port) {

		ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager = selectFirst(
				replicaSetGroup.getServers(),
				having(on(ManagedMongoDbLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedMongoDbLifecycleManager.class).isReady(), is(true))));

		if (managedMongoDbLifecycleManager != null) {
			managedMongoDbLifecycleManager.stopEngine();
		}

	}

	public void startupServer(int port) throws Throwable {

		ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager = selectFirst(
				replicaSetGroup.getServers(),
				having(on(ManagedMongoDbLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedMongoDbLifecycleManager.class).isReady(), is(false))));

		if (managedMongoDbLifecycleManager != null) {
			managedMongoDbLifecycleManager.startEngine();
		}

	}
	
	@Override
	protected void before() throws Throwable {
		wakeUpServers();
		replicaSetInitiate();
		waitUntilConfigurationSpreadAcrossServers();
	}

	private void waitUntilConfigurationSpreadAcrossServers() {

		MongoClient mongoClient;
		try {
			mongoClient = getMongoClient();
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}

		boolean isConfigurationSpread = false;

		int retries = 0;

		while (!isConfigurationSpread) {

			try {
				TimeUnit.SECONDS.sleep(WAIT_TIME);
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}

			DBObject status = getMongosStatus(mongoClient);
			isConfigurationSpread = isConfigurationSpread(status);

			retries++;

			if (retries > MAX_RETRIES && !isConfigurationSpread) {
				throw new IllegalStateException(
						"After "
								+ (WAIT_TIME * MAX_RETRIES)
								+ " seconds replica set scenario could not be started and configured. Last status message was: "
								+ JSON.serialize(status));
			}

		}

	}

	private boolean isConfigurationSpread(DBObject status) {

		BasicDBList members = (BasicDBList) status.get(MEMBERS_TOKEN);

		if(members == null) {
			return false;
		}
		
		for (Object object : members) {

			DBObject member = (DBObject) object;

			if (member.containsField(ERRMSG_TOKEN)
					|| member.get(STATE_TOKEN).equals(UNKNOWN_TOKEN)) {
				return false;
			}
		}

		return true;
	}

	private DBObject getMongosStatus(MongoClient mongoClient) {
		DBObject status = null;

		if (this.replicaSetGroup.isAuthenticationSet()) {
			status = MongoDBCommands.replicaSetGetStatus(mongoClient,
					this.replicaSetGroup.getUsername(),
					this.replicaSetGroup.getPassword());
		} else {
			status = MongoDBCommands.replicaSetGetStatus(mongoClient);
		}
		return status;
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

		CommandResult commandResult = runCommandToAdmin(getConfigurationDocument());

		LOGGER.info("Command {} has returned {}", "replSetInitiaite",
				commandResult.toString());

	}

	private CommandResult runCommandToAdmin(ConfigurationDocument cmd)
			throws UnknownHostException {
		MongoClient mongoClient = getMongoClient();

		if (this.replicaSetGroup.isAuthenticationSet()) {
			return MongoDBCommands.replicaSetInitiate(mongoClient, cmd,
					this.replicaSetGroup.getUsername(),
					this.replicaSetGroup.getPassword());
		} else {
			return MongoDBCommands.replicaSetInitiate(mongoClient, cmd);
		}

	}

	private void shutdownServers() {
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup
				.getServers()) {
			managedMongoDbLifecycleManager.stopEngine();
		}
	}

	private void wakeUpServers() throws Throwable {
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : replicaSetGroup
				.getServers()) {
			if (isServerStopped(managedMongoDbLifecycleManager)) {
				managedMongoDbLifecycleManager.startEngine();
			}
		}
	}

	private boolean isServerStopped(
			ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return !managedMongoDbLifecycleManager.isReady();
	}

	private MongoClient getMongoClient() throws UnknownHostException {

		ManagedMongoDbLifecycleManager defaultConnection = replicaSetGroup
				.getDefaultConnection();
		return new MongoClient(defaultConnection.getHost(),
				defaultConnection.getPort());

	}

}
