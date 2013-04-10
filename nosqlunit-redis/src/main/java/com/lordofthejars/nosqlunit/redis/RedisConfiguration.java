package com.lordofthejars.nosqlunit.redis;

public class RedisConfiguration extends AbstractRedisConfiguration {

	private String host;
	private int port;
	private String password;



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
}
