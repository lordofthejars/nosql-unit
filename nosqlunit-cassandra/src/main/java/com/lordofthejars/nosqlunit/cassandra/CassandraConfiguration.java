package com.lordofthejars.nosqlunit.cassandra;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class CassandraConfiguration extends AbstractJsr330Configuration {

	private String clusterName;
	private String host;
	private int port;
	
	public CassandraConfiguration() {
		super();
	}
	
	public CassandraConfiguration(String clusterName, String host, int port) {
		super();
		this.clusterName = clusterName;
		this.host = host;
		this.port = port;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	
}
