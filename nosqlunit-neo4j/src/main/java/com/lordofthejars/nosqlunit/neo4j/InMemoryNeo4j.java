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

public class InMemoryNeo4j extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryNeo4j.class); 
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;

	public static final String INMEMORY_NEO4J_TARGET_PATH = "target" + File.separatorChar + "test-data"
			+ File.separatorChar + "impermanent-db";

	private Map<String, String> configurationParameters = new HashMap<String, String>();
	private GraphDatabaseService graphDb;

	private InMemoryNeo4j() {
		super();
	}

	public static class InMemoryNeo4jRuleBuilder {

		private InMemoryNeo4j inMemoryNeo4j;

		private InMemoryNeo4jRuleBuilder() {
			this.inMemoryNeo4j = new InMemoryNeo4j();
		}

		public static InMemoryNeo4jRuleBuilder newInMemoryNeo4j() {
			return new InMemoryNeo4jRuleBuilder();
		}

		public InMemoryNeo4jRuleBuilder configuration(Map<String, String> parameters) {
			this.inMemoryNeo4j.configurationParameters.putAll(parameters);
			return this;
		}

		public InMemoryNeo4j build() {
			return this.inMemoryNeo4j;
		}

	}

	@Override
	protected String getHost() {
		return LOCALHOST + INMEMORY_NEO4J_TARGET_PATH;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	protected void doStart() throws Throwable {
		
		LOGGER.info("Starting Embedded InMemory Neo4j instance.");
		
		createInMemoryGraphDatabaseService();
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, INMEMORY_NEO4J_TARGET_PATH);
		
		LOGGER.info("Started Embedded InMemory Neo4j instance.");
	}

	@Override
	protected void doStop() {
		
		LOGGER.info("Stopping Embedded InMemory Neo4j instance.");
		
		this.graphDb.shutdown();
		EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService(INMEMORY_NEO4J_TARGET_PATH);
		
		LOGGER.info("Stopped Embedded InMemory Neo4j instance.");
	}

	private void createInMemoryGraphDatabaseService() {
		this.graphDb = new ImpermanentGraphDatabase(configurationParameters);
	}


}
