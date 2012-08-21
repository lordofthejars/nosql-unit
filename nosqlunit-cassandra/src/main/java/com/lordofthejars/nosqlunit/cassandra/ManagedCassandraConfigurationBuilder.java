package com.lordofthejars.nosqlunit.cassandra;

import me.prettyprint.cassandra.service.CassandraHost;

public class ManagedCassandraConfigurationBuilder {

	private static final String LOCALHOST = "127.0.0.1";
	private static final int DEFAULT_PORT= CassandraHost.DEFAULT_PORT;
	
	private CassandraConfiguration cassandraConfiguration;
	
	private ManagedCassandraConfigurationBuilder() {
		this.cassandraConfiguration = new CassandraConfiguration();
		this.cassandraConfiguration.setHost(LOCALHOST);
		this.cassandraConfiguration.setPort(DEFAULT_PORT);
	}
	
	public static ManagedCassandraConfigurationBuilder newManagedCassandraConfiguration() {
		return new ManagedCassandraConfigurationBuilder();
	}
	
	public ManagedCassandraConfigurationBuilder port(int port) {
		this.cassandraConfiguration.setPort(port);
		return this;
	}
	
	public ManagedCassandraConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.cassandraConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public ManagedCassandraConfigurationBuilder clusterName(String clusterName) {
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
