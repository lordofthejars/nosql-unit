package com.lordofthejars.nosqlunit.cassandra;

public class EmbeddedCassandraConfigurationBuilder {

	private static final String LOCALHOST = "127.0.0.1";
	private static final int DEFAULT_PORT= EmbeddedCassandra.DEFAULT_PORT;
	
	private CassandraConfiguration cassandraConfiguration;
	
	private EmbeddedCassandraConfigurationBuilder() {
		this.cassandraConfiguration = new CassandraConfiguration();
		this.cassandraConfiguration.setHost(LOCALHOST);
		this.cassandraConfiguration.setPort(DEFAULT_PORT);
	}
	
	public static EmbeddedCassandraConfigurationBuilder newEmbeddedCassandraConfiguration() {
		return new EmbeddedCassandraConfigurationBuilder();
	}
	
	public EmbeddedCassandraConfigurationBuilder port(int port) {
		this.cassandraConfiguration.setPort(port);
		return this;
	}
	
	public EmbeddedCassandraConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.cassandraConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public EmbeddedCassandraConfigurationBuilder clusterName(String clusterName) {
		this.cassandraConfiguration.setClusterName(clusterName);
		return this;
	}
	
	public CassandraConfiguration build() {
		if(this.cassandraConfiguration.getClusterName() == null) {
			throw new IllegalArgumentException("Cluster name cannot be null");
		}
		return this.cassandraConfiguration;
	}
}
