package com.lordofthejars.nosqlunit.neo4j;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.server.configuration.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedNeo4jLifecycleManager extends AbstractLifecycleManager {

private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedNeo4j.class); 
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;

	protected static final String DEFAULT_NEO4J_TARGET_PATH = "target" + File.separatorChar + "neo4j-temp";

	private String targetPath = DEFAULT_NEO4J_TARGET_PATH;
	private GraphDatabaseService graphDb;

	public EmbeddedNeo4jLifecycleManager() {
		super();
	}

	
	@Override
	public String getHost() {
		return LOCALHOST+targetPath;
	}

	@Override
	public int getPort() {
		return PORT;
	}

	@Override
	public void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Neo4j instance.");
		
		cleanDb();
		createEmbeddedGraphDatabaseService();
		
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, targetPath);
		registerShutdownHook(graphDb);
		
		LOGGER.info("Started Embedded Neo4j instance.");
	}

	@Override
	public void doStop() {
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

	public void setTargetPath(String targetPath) {
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
	
	public String getTargetPath() {
		return targetPath;
	}


}
