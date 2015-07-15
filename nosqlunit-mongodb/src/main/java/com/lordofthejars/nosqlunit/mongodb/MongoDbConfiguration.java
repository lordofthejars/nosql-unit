package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public final class MongoDbConfiguration extends AbstractJsr330Configuration {
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 27017;
	private String databaseName;
	
	private String username;
	private String password;
	
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	
	private Mongo mongo;
	
	private WriteConcern writeConcern = WriteConcern.SAFE;
	
	public MongoDbConfiguration() {
		super();
	}
	
	public MongoDbConfiguration(String host, String databaseName) {
		super();
		this.databaseName = databaseName;
		this.host = host;
	}

	public MongoDbConfiguration(String databaseName, String username,
			String password) {
		super();
		this.databaseName = databaseName;
		this.username = username;
		this.password = password;
	}
	
	
	public boolean isAuthenticateParametersSet() {
		return this.username != null && this.password !=null; 
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public int getPort() {
		return port;
	}
	
	public String getHost() {
		return host;
	}
	
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}
	
	public Mongo getMongo() {
		return mongo;
	}
	
	public WriteConcern getWriteConcern() {
		return writeConcern;
	}
	
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}
	
}
