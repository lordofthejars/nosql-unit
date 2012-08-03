	package com.lordofthejars.nosqlunit.neo4j;

import org.neo4j.rest.graphdb.RestGraphDatabase;

public class ManagedNeoServerConfigurationBuilder {

	private final Neo4jConfiguration neo4jConfiguration;
	
	private ManagedNeoServerConfigurationBuilder() {
		super();
		this.neo4jConfiguration = new Neo4jConfiguration();
	}

	public static ManagedNeoServerConfigurationBuilder newManagedNeoServerConfiguration() {
		return new ManagedNeoServerConfigurationBuilder();
	}
	
	public ManagedNeoServerConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.neo4jConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public ManagedNeoServerConfigurationBuilder uri(String uri) {
		this.neo4jConfiguration.setUri(uri);
		return this;
	}
	
	public ManagedNeoServerConfigurationBuilder username(String username) {
		this.neo4jConfiguration.setUserName(username);
		return this;
	}
	
	public ManagedNeoServerConfigurationBuilder password(String password) {
		this.neo4jConfiguration.setPassword(password);
		return this;
	}
	
	public Neo4jConfiguration build() {
		this.neo4jConfiguration.setGraphDatabaseService(new RestGraphDatabase(this.neo4jConfiguration.getUri(), this.neo4jConfiguration.getUserName(), this.neo4jConfiguration.getPassword()));
		return this.neo4jConfiguration;
	}
	
}

