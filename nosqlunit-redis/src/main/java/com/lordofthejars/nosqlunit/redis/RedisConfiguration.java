package com.lordofthejars.nosqlunit.redis;

public class RedisConfiguration extends AbstractRedisConfiguration {

	private static final int NO_MASTER_PORT = -1;
	private String host;
	private int port;
	private String password;

	private String masterHost;
	private int masterPort = NO_MASTER_PORT;

	public RedisConfiguration() {
		super();
	}

	public RedisConfiguration(String host, int port, String password) {
		super();
		this.host = host;
		this.port = port;
		this.password = password;
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

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void salveOf(String host, int port) {
		this.masterHost = host;
		this.masterPort = port;
	}

	public String getMasterHost() {
		return masterHost;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public boolean isSlave() {
		return getMasterHost() != null && getMasterPort() != NO_MASTER_PORT;
	}
	
}
