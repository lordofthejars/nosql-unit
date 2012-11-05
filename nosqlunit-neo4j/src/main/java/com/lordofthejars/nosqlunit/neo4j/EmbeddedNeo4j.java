package com.lordofthejars.nosqlunit.neo4j;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.server.configuration.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedNeo4j extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedNeo4j.class); 
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;

	protected static final String DEFAULT_NEO4J_TARGET_PATH = "target" + File.separatorChar + "neo4j-temp";

	private String targetPath = DEFAULT_NEO4J_TARGET_PATH;
	private GraphDatabaseService graphDb;

	private EmbeddedNeo4j() {
		super();
	}

	public static class EmbeddedNeo4jRuleBuilder {

		private EmbeddedNeo4j embeddedNeo4j;

		private EmbeddedNeo4jRuleBuilder() {
			this.embeddedNeo4j = new EmbeddedNeo4j();
		}

		public static EmbeddedNeo4jRuleBuilder newEmbeddedNeo4jRule() {
			return new EmbeddedNeo4jRuleBuilder();
		}

		public EmbeddedNeo4jRuleBuilder targetPath(String targetPath) {
			this.embeddedNeo4j.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedNeo4j build() {
			if (this.embeddedNeo4j.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Neo4j is provided.");
			}
			return this.embeddedNeo4j;
		}

	}

	@Override
	protected String getHost() {
		return LOCALHOST+targetPath;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	protected void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Neo4j instance.");
		
		cleanDb();
		createEmbeddedGraphDatabaseService();
		
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, targetPath);
		registerShutdownHook(graphDb);
		
		LOGGER.info("Started Embedded Neo4j instance.");
	}

	@Override
	protected void doStop() {
		LOGGER.info("Stopping Embedded Neo4j instance.");
		
		shutdownGraphDb();
		
		LOGGER.info("Stopped Embedded Neo4j instance.");
	}

	private void shutdownGraphDb() {
		try {
			this.graphDb.shutdown();
			EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService(targetPath);
		} finally {
			cleanDb();
		}
	}


	private void createEmbeddedGraphDatabaseService() {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(this.targetPath);
	}
	
	private void cleanDb() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
	}

	private void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	private void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
	
	private String getTargetPath() {
		return targetPath;
	}

	
	
}
