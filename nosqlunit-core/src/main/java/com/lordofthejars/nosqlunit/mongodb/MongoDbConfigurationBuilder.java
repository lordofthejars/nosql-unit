package com.lordofthejars.nosqlunit.mongodb;


public class MongoDbConfigurationBuilder {

	public static MongoDbConfigurationBuilder mongoDb() {
		return new MongoDbConfigurationBuilder();
	}
	
	private final MongoDbConfiguration mongoDbConfiguration;
	
	private MongoDbConfigurationBuilder() {
		mongoDbConfiguration = new MongoDbConfiguration();
	}
	
	public MongoDbConfiguration build() {
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
	
}
