package com.lordofthejars.nosqlunit.neo4j;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class InMemoryNeo4jLifecycleManager extends AbstractLifecycleManager {

private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryNeo4j.class); 
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;

	public static final String INMEMORY_NEO4J_TARGET_PATH = "target" + File.separatorChar + "test-data"
			+ File.separatorChar + "impermanent-db";

	private Map<String, String> configurationParameters = new HashMap<String, String>();
	private GraphDatabaseService graphDb;

	public InMemoryNeo4jLifecycleManager() {
		super();
	}

	@Override
	public String getHost() {
		return LOCALHOST + INMEMORY_NEO4J_TARGET_PATH;
	}

	@Override
	public int getPort() {
		return PORT;
	}

	@Override
	public void doStart() throws Throwable {
		
		LOGGER.info("Starting Embedded InMemory Neo4j instance.");
		
		createInMemoryGraphDatabaseService();
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, INMEMORY_NEO4J_TARGET_PATH);
		
		LOGGER.info("Started Embedded InMemory Neo4j instance.");
	}

	@Override
	public void doStop() {
		
		LOGGER.info("Stopping Embedded InMemory Neo4j instance.");
		
		this.graphDb.shutdown();
		EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService(INMEMORY_NEO4J_TARGET_PATH);
		
		LOGGER.info("Stopped Embedded InMemory Neo4j instance.");
	}

	private void createInMemoryGraphDatabaseService() {
		this.graphDb = new ImpermanentGraphDatabase(configurationParameters);
	}


	public void setConfigurationParameters(Map<String, String> configurationParameters) {
		this.configurationParameters = configurationParameters;
	}
	
	public Map<String, String> getConfigurationParameters() {
		return configurationParameters;
	}
	
}
