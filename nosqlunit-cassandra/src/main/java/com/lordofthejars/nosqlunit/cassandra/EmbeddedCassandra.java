package com.lordofthejars.nosqlunit.cassandra;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.rules.ExternalResource;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;

public class EmbeddedCassandra extends ExternalResource {

	protected static final String DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION = "cu-cassandra.yaml";
	
	protected static final String LOCALHOST = "localhost";
	protected static final int PORT = 9171;
	
	protected static final String DEFAULT_CASSANDRA_TARGET_PATH = "target" + File.separatorChar + "cassandra-temp";
	
	private String targetPath = DEFAULT_CASSANDRA_TARGET_PATH;
	private String cassandraConfigurationFile = DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION;
	
	private EmbeddedCassandraServerHelper embeddedCassandraServerHelper = new EmbeddedCassandraServerHelper();
	
	private EmbeddedCassandra() {
		super();
	}
	
	public static class EmbeddedCassandraRuleBuilder {

		private EmbeddedCassandra embeddedCassandra;

		private EmbeddedCassandraRuleBuilder() {
			this.embeddedCassandra = new EmbeddedCassandra();
		}

		public static EmbeddedCassandraRuleBuilder newEmbeddedCassandraRule() {
			return new EmbeddedCassandraRuleBuilder();
		}

		public EmbeddedCassandraRuleBuilder targetPath(String targetPath) {
			this.embeddedCassandra.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedCassandraRuleBuilder cassandraConfigurationPath(String cassandraConfigurationPath) {
			this.embeddedCassandra.setCassandraConfigurationFile(cassandraConfigurationPath);
			return this;
		}
		
		public EmbeddedCassandra build() {
			
			if (this.embeddedCassandra.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Cassandra is provided.");
			}
			
			return this.embeddedCassandra;
		}

	}
	
	@Override
	protected void before() throws Throwable {
		if (isServerNotStartedYet()) {
			createEmbeddedCassandra();
		}

		ConnectionManagement.getInstance().addConnection(LOCALHOST, PORT);
	}

	private void createEmbeddedCassandra() throws TTransportException, IOException, InterruptedException, ConfigurationException {
		embeddedCassandraServerHelper.startEmbeddedCassandra(cassandraConfigurationFile, targetPath);
	}

	@Override
	protected void after() {
		int remainingConnections = ConnectionManagement.getInstance()
				.removeConnection(LOCALHOST, PORT);
		if (noMoreConnectionsToManage(remainingConnections)) {
			stopEmbeddedCassandra();
		}
	}

	private void stopEmbeddedCassandra() {
		embeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

	private boolean noMoreConnectionsToManage(int remainingConnections) {
		return remainingConnections < 1;
	}

	
	private boolean isServerNotStartedYet() {
		return !ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT);
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
	
	public void setCassandraConfigurationFile(String cassandraConfigurationFile) {
		this.cassandraConfigurationFile = cassandraConfigurationFile;
	}
	
	protected void setEmbeddedCassandraServerHelper(EmbeddedCassandraServerHelper embeddedCassandraServerHelper) {
		this.embeddedCassandraServerHelper = embeddedCassandraServerHelper;
	}
	
}
