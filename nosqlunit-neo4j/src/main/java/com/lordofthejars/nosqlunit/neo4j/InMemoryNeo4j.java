package com.lordofthejars.nosqlunit.neo4j;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.ExternalResource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.ImpermanentGraphDatabase;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;

public class InMemoryNeo4j extends ExternalResource {

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
	protected void before() throws Throwable {
		if (isServerNotStartedYet()) {
			createInMemoryGraphDatabaseService();
			EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, INMEMORY_NEO4J_TARGET_PATH);
		}

		ConnectionManagement.getInstance().addConnection(LOCALHOST + INMEMORY_NEO4J_TARGET_PATH, PORT);
	}

	private void createInMemoryGraphDatabaseService() {
		this.graphDb = new ImpermanentGraphDatabase(configurationParameters);
	}

	@Override
	protected void after() {
		int remainingConnections = ConnectionManagement.getInstance().removeConnection(
				LOCALHOST + INMEMORY_NEO4J_TARGET_PATH, PORT);
		if (noMoreConnectionsToManage(remainingConnections)) {
			this.graphDb.shutdown();
			EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService(INMEMORY_NEO4J_TARGET_PATH);
		}
	}

	private boolean noMoreConnectionsToManage(int remainingConnections) {
		return remainingConnections < 1;
	}

	private boolean isServerNotStartedYet() {
		return !ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST + INMEMORY_NEO4J_TARGET_PATH, PORT);
	}

}
