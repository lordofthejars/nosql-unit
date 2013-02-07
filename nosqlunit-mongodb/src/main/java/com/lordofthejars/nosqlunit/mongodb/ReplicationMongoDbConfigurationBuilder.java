package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DBPort;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class ReplicationMongoDbConfigurationBuilder {

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = DBPort.PORT;

	private MongoDbConfiguration mongoDbConfiguration = new MongoDbConfiguration();
	private List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();

	private boolean enableSharding = false;

	private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

	private ReplicationMongoDbConfigurationBuilder() {
		super();
	}

	public static ReplicationMongoDbConfigurationBuilder replicationMongoDbConfiguration() {
		return new ReplicationMongoDbConfigurationBuilder();
	}

	public ReplicationMongoDbConfigurationBuilder seed(String host, int port) {
		try {
			serverAddresses.add(new ServerAddress(host, port));
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder databaseName(
			String databaseName) {
		mongoDbConfiguration.setDatabaseName(databaseName);
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder username(String username) {
		mongoDbConfiguration.setUsername(username);
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder password(String password) {
		mongoDbConfiguration.setPassword(password);
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder connectionIdentifier(
			String identifier) {
		mongoDbConfiguration.setConnectionIdentifier(identifier);
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder enableSharding() {
		this.enableSharding = true;
		return this;
	}

	public ReplicationMongoDbConfigurationBuilder writeConcern(
			WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
		return this;
	}

	public MongoDbConfiguration configure() {

		if (this.serverAddresses.isEmpty()) {
			addDefaultSeed();
		}

		
		MongoClient mongoClient = new MongoClient(this.serverAddresses);

		if(this.enableSharding) {
			enableSharding(mongoClient);
		}
		
		mongoClient.setWriteConcern(writeConcern);
		mongoDbConfiguration.setMongo(mongoClient);

		return mongoDbConfiguration;
	}

	private void enableSharding(MongoClient mongoClient) {
		if (isAuthenticationSet()) {
			MongoDbCommands.enableSharding(mongoClient,
					this.mongoDbConfiguration.getDatabaseName(),
					this.mongoDbConfiguration.getUsername(),
					this.mongoDbConfiguration.getPassword());
		} else {
			MongoDbCommands.enableSharding(mongoClient,
					this.mongoDbConfiguration.getDatabaseName());
		}
	}

	private boolean isAuthenticationSet() {
		return this.mongoDbConfiguration.getUsername() != null
				&& this.mongoDbConfiguration.getPassword() != null;
	}

	private void addDefaultSeed() {
		try {
			serverAddresses.add(new ServerAddress(DEFAULT_HOST, DEFAULT_PORT));
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
