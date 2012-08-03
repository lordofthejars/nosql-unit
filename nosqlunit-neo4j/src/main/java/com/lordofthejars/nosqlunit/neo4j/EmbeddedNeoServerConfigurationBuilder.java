package com.lordofthejars.nosqlunit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;

import com.lordofthejars.nosqlunit.core.FailureHandler;


public class EmbeddedNeoServerConfigurationBuilder {

	private final Neo4jConfiguration neo4jConfiguration;
	
	private EmbeddedNeoServerConfigurationBuilder() {
		super();
		this.neo4jConfiguration = new Neo4jConfiguration();
	}
	
	public static EmbeddedNeoServerConfigurationBuilder newEmbeddedNeoServerConfiguration() {
		return new EmbeddedNeoServerConfigurationBuilder();
	}
	
	public EmbeddedNeoServerConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.neo4jConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public Neo4jConfiguration build() {
		GraphDatabaseService defaultGraphDatabaseService = EmbeddedNeo4jInstances.getInstance().getDefaultGraphDatabaseService();
		
		if(defaultGraphDatabaseService == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedNeo4j rule during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		this.neo4jConfiguration.setGraphDatabaseService(defaultGraphDatabaseService);
		return this.neo4jConfiguration;
	}
	
	public Neo4jConfiguration buildFromTargetPath(String targetPath) {
		GraphDatabaseService graphDatabaseService = EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(targetPath);
		
		if(graphDatabaseService == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedNeo4j rule with %s target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.", targetPath);
		}
		
		this.neo4jConfiguration.setGraphDatabaseService(graphDatabaseService);
		return this.neo4jConfiguration;
	}
	
	
	
}
