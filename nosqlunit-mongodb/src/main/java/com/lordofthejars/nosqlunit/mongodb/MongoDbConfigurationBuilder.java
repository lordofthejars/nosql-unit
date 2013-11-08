package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;

import com.mongodb.MongoClient;


public class MongoDbConfigurationBuilder {

	public static MongoDbConfigurationBuilder mongoDb() {
		return new MongoDbConfigurationBuilder();
	}
	
	private final MongoDbConfiguration mongoDbConfiguration;
	
	private MongoDbConfigurationBuilder() {
		mongoDbConfiguration = new MongoDbConfiguration();
	}
	
	public MongoDbConfiguration build() {
		
		try {
			MongoClient mongo = new MongoClient(this.mongoDbConfiguration.getHost(), this.mongoDbConfiguration.getPort());
			this.mongoDbConfiguration.setMongo(mongo);
		} catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
		
		return mongoDbConfiguration;
	}
	
	public MongoDbConfigurationBuilder databaseName(String databaseName) {
		mongoDbConfiguration.setDatabaseName(databaseName);
		return this;
	}

	public MongoDbConfigurationBuilder port(int port) {
		mongoDbConfiguration.setPort(port);
		return this;
	}
	
	public MongoDbConfigurationBuilder username(String username) {
		mongoDbConfiguration.setUsername(username);
		return this;
	}
	
	public MongoDbConfigurationBuilder password(String password) {
		mongoDbConfiguration.setPassword(password);
		return this;
	}
	
	public MongoDbConfigurationBuilder host(String host) {
		mongoDbConfiguration.setHost(host);
		return this;
	}
	
	public MongoDbConfigurationBuilder connectionIdentifier(String identifier) {
		mongoDbConfiguration.setConnectionIdentifier(identifier);
		return this;
	}
	
}
