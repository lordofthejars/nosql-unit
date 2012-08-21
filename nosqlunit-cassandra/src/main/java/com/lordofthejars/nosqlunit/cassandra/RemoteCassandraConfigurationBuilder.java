package com.lordofthejars.nosqlunit.cassandra;

import me.prettyprint.cassandra.service.CassandraHost;

public class RemoteCassandraConfigurationBuilder {

	
	private static final int DEFAULT_PORT= CassandraHost.DEFAULT_PORT;
	
	private CassandraConfiguration cassandraConfiguration;
	
	private RemoteCassandraConfigurationBuilder() {
		this.cassandraConfiguration = new CassandraConfiguration();
		this.cassandraConfiguration.setPort(DEFAULT_PORT);
	}
	
	public static RemoteCassandraConfigurationBuilder newRemoteCassandraConfiguration() {
		return new RemoteCassandraConfigurationBuilder();
	}
	
	public RemoteCassandraConfigurationBuilder host(String host) {
		this.cassandraConfiguration.setHost(host);
		return this;
	}
	
	public RemoteCassandraConfigurationBuilder port(int port) {
		this.cassandraConfiguration.setPort(port);
		return this;
	}
	
	public RemoteCassandraConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.cassandraConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public RemoteCassandraConfigurationBuilder clusterName(String clusterName) {
		this.cassandraConfiguration.setClusterName(clusterName);
		return this;
	}
	
	
	
	public CassandraConfiguration build() {
		if(this.cassandraConfiguration.getClusterName() == null) {
			throw new IllegalArgumentException("Cluster name cannot be null");
		}
		
		if(this.cassandraConfiguration.getHost() == null) {
			throw new IllegalArgumentException("Host cannot be null");
		}
		
		return this.cassandraConfiguration;
	}
	
}
