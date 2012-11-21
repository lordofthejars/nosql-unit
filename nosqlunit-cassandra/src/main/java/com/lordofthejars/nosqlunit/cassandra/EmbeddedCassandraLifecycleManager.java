package com.lordofthejars.nosqlunit.cassandra;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedCassandraLifecycleManager extends AbstractLifecycleManager {

private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedCassandra.class); 
	
	protected static final String DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION = "cu-cassandra.yaml";
	
	protected static final String LOCALHOST = "localhost";
	protected static final int DEFAULT_PORT = 9171;
	
	protected static final String DEFAULT_CASSANDRA_TARGET_PATH = "target" + File.separatorChar + "cassandra-temp";
	
	private String targetPath = DEFAULT_CASSANDRA_TARGET_PATH;
	private String cassandraConfigurationFile = DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION;
	private int port = DEFAULT_PORT;
	
	private EmbeddedCassandraServerHelper embeddedCassandraServerHelper = new EmbeddedCassandraServerHelper();
	
	public EmbeddedCassandraLifecycleManager() {
		super();
	}
	
	
	@Override
	protected String getHost() {
		return LOCALHOST;
	}

	@Override
	protected int getPort() {
		return port;
	}

	@Override
	protected void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Cassandra instance.");
		createEmbeddedCassandra();
		LOGGER.info("Started Embedded Cassandra instance.");
	}

	@Override
	protected void doStop() {
		LOGGER.info("Stopping Embedded Cassandra instance.");
		stopEmbeddedCassandra();
		LOGGER.info("Stopped Embedded Cassandra instance.");
	}

	private void createEmbeddedCassandra() throws TTransportException, IOException, InterruptedException, ConfigurationException {
		embeddedCassandraServerHelper.startEmbeddedCassandra(cassandraConfigurationFile, targetPath);
	}

	private void stopEmbeddedCassandra() {
		embeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

	public String getTargetPath() {
		return targetPath;
	}
	
	public String getCassandraConfigurationFile() {
		return cassandraConfigurationFile;
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	
	public void setCassandraConfigurationFile(String cassandraConfigurationFile) {
		this.cassandraConfigurationFile = cassandraConfigurationFile;
	}
	
	protected void setEmbeddedCassandraServerHelper(EmbeddedCassandraServerHelper embeddedCassandraServerHelper) {
		this.embeddedCassandraServerHelper = embeddedCassandraServerHelper;
	}

}
