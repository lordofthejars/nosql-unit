package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.Arrays;


public class MongoDbConfigurationBuilder {

	public static MongoDbConfigurationBuilder mongoDb() {
		return new MongoDbConfigurationBuilder();
	}
	
	private final MongoDbConfiguration mongoDbConfiguration;
	
	private MongoDbConfigurationBuilder() {
		mongoDbConfiguration = new MongoDbConfiguration();
	}
	
	public MongoDbConfiguration build() {
		MongoClient mongo = null;
		if(this.mongoDbConfiguration.isAuthenticateParametersSet()) {
			MongoCredential credential = MongoCredential.createCredential(this.mongoDbConfiguration.getUsername(),
					this.mongoDbConfiguration.getDatabaseName(),
					this.mongoDbConfiguration.getPassword().toCharArray());
			mongo = new MongoClient(new ServerAddress(this.mongoDbConfiguration.getHost(), this.mongoDbConfiguration.getPort()), Arrays.asList(credential));
		} else {
			mongo = new MongoClient(this.mongoDbConfiguration.getHost(), this.mongoDbConfiguration.getPort());
		}
		this.mongoDbConfiguration.setMongo(mongo);

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
