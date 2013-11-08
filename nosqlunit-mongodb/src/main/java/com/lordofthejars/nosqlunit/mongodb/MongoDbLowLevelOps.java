package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class MongoDbLowLevelOps {

	private static final String MEMBERS_TOKEN = "members";

	private static final int WAIT_TIME = 6;
	private static final int MAX_RETRIES = 20;

	private static final String STATE_TOKEN = "state";

	private static final Integer STARTING_UP_1 = 0;
	private static final Integer PRIMARY = 1;
	private static final Integer SECONDARY = 2;
	private static final Integer RECOVERING = 3;
	private static final Integer FATAL_ERROR = 4;
	private static final Integer STARTING_UP_2 = 5;
	private static final Integer UNKNOWN = 6;
	private static final Integer ARBITER = 7;
	private static final Integer DOWN = 8;

	MongoDbLowLevelOps() {
		super();
	}

	public void waitUntilReplicaSetBecomeStable(MongoClient mongoClient,
			int numberOfServersStable, String... authenticateParameters) {

		boolean isConfigurationSpread = false;

		int retries = 0;

		while (!isConfigurationSpread) {

			try {
				TimeUnit.SECONDS.sleep(WAIT_TIME);
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}

			DBObject status = null;
			try {
				status = getStatus(mongoClient, authenticateParameters);
				isConfigurationSpread = isSystemStable(status,
						numberOfServersStable);
			} catch (MongoException e) {
				status = new BasicDBObject("MongoException", "can't find a master");
				isConfigurationSpread = false;
			}

			retries++;

			if (retries > MAX_RETRIES && !isConfigurationSpread) {
				mongoClient.close();
				throw new IllegalStateException(
						"After "
								+ (WAIT_TIME * MAX_RETRIES)
								+ " seconds replica set scenario could not be started and configured. Last status message was: "
								+ JSON.serialize(status));
			}

		}

	}

	private DBObject getStatus(MongoClient mongoClient,
			String... authenticateParameters) {
		DBObject status = null;
		if (authenticateParameters.length == 2) {
			status = getMongosStatus(mongoClient, authenticateParameters[0],
					authenticateParameters[1]);
		} else {
			status = getMongosStatus(mongoClient);
		}
		return status;
	}

	private boolean isSystemStable(DBObject status, int numberOfServersStable) {

		int currentNumberOfStables = 0;

		BasicDBList members = (BasicDBList) status.get(MEMBERS_TOKEN);

		if (members == null) {
			return false;
		}

		for (Object object : members) {

			DBObject member = (DBObject) object;

			if (member.get(STATE_TOKEN).equals(PRIMARY)
					|| member.get(STATE_TOKEN).equals(SECONDARY)
					|| member.get(STATE_TOKEN).equals(ARBITER)) {
				currentNumberOfStables++;
			}
		}

		return currentNumberOfStables == numberOfServersStable;
	}

	private DBObject getMongosStatus(MongoClient mongoClient, String username,
			String password) {
		return MongoDbCommands.replicaSetGetStatus(mongoClient, username,
				password);
	}

	private DBObject getMongosStatus(MongoClient mongoClient) {
		return MongoDbCommands.replicaSetGetStatus(mongoClient);
	}

	public boolean assertThatConnectionIsPossible(String host, int port) throws InterruptedException, UnknownHostException,
			MongoException {

		int currentRetry = 0;
		boolean connectionIsPossible = false;

		Mongo server = null;
		try {
			do {
				TimeUnit.SECONDS.sleep(WAIT_TIME);
				server = new MongoClient(host, port);
				DB db = server.getDB("admin");
				try {
					db.getStats();
					connectionIsPossible = true;
				} catch (MongoException e) {
					currentRetry++;
				}
			} while (!connectionIsPossible && currentRetry <= MAX_RETRIES);
		} finally {
			server.close();
		}

		return connectionIsPossible;
	}

	public void shutdown(String host, int port) {
		MongoDbCommands.shutdown(host, port);
	}

}
