package com.lordofthejars.nosqlunit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;

import com.lordofthejars.nosqlunit.core.Configuration;

public class Neo4jConfiguration implements Configuration {

	protected static final String DEFAULT_URI = "http://localhost:7474/db/data";
	
	private String connectionIdentifier = "";
	
	private String uri = DEFAULT_URI;
	
	private String userName = null;
	private String password = null;
	
	private GraphDatabaseService graphDatabaseService;
	
	public Neo4jConfiguration() {
		super();
	}

	public Neo4jConfiguration(String uri) {
		super();
		this.uri = uri;
	}

	public Neo4jConfiguration(String uri, String userName, String password) {
		super();
		this.uri = uri;
		this.userName = userName;
		this.password = password;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setGraphDatabaseService(GraphDatabaseService graphDatabaseService) {
		this.graphDatabaseService = graphDatabaseService;
	}
	
	public GraphDatabaseService getGraphDatabaseService() {
		return graphDatabaseService;
	}
	
	public String getConnectionIdentifier() {
		return connectionIdentifier;
	}
	
	public void setConnectionIdentifier(String connectionIdentifier) {
		this.connectionIdentifier = connectionIdentifier;
	}
	
}
