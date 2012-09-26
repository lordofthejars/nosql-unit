package com.lordofthejars.nosqlunit.cassandra;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.transport.TTransportException;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedCassandra extends AbstractLifecycleManager {

	protected static final String DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION = "cu-cassandra.yaml";
	
	protected static final String DEFAULT_HOST = "localhost";
	protected static final int DEFAULT_PORT = 9171;
	
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
	protected String getHost() {
		return DatabaseDescriptor.getRpcAddress().getHostName();
	}

	@Override
	protected int getPort() {
        return DatabaseDescriptor.getRpcPort();
	}

	@Override
	protected void doStart() throws Throwable {
		createEmbeddedCassandra();
	}

	@Override
	protected void doStop() {
		stopEmbeddedCassandra();
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
	
	public void setCassandraConfigurationFile(String cassandraConfigurationFile) {
		this.cassandraConfigurationFile = cassandraConfigurationFile;
	}
	
	protected void setEmbeddedCassandraServerHelper(EmbeddedCassandraServerHelper embeddedCassandraServerHelper) {
		this.embeddedCassandraServerHelper = embeddedCassandraServerHelper;
	}
	
}
