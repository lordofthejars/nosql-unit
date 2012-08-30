package com.lordofthejars.nosqlunit.redis;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class RedisConfiguration extends AbstractJsr330Configuration {

	private String host;
	private int port;
	private String password;
	
	private Jedis jedis;
	
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
	
	public void setJedis(Jedis jedis) {
		this.jedis = jedis;
	}
	
	public Jedis getJedis() {
		return jedis;
	}
	
}
