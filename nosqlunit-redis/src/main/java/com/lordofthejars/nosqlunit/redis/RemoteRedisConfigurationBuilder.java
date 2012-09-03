package com.lordofthejars.nosqlunit.redis;

import redis.clients.jedis.Jedis;

public class RemoteRedisConfigurationBuilder {

	private static final int DEFAULT_PORT = ManagedRedis.DEFAULT_PORT;
	
	private RedisConfiguration redisConfiguration;
	
	private RemoteRedisConfigurationBuilder() {
		redisConfiguration = new RedisConfiguration();
		redisConfiguration.setPort(DEFAULT_PORT);
	}
	
	public static RemoteRedisConfigurationBuilder newRemoteRedisConfiguration() {
		return new RemoteRedisConfigurationBuilder();
	}
	
	public RemoteRedisConfigurationBuilder host(String host) {
		this.redisConfiguration.setHost(host);
		return this;
	}
	
	public RemoteRedisConfigurationBuilder port(int port) {
		this.redisConfiguration.setPort(port);
		return this;
	}
	
	public RemoteRedisConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.redisConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public RemoteRedisConfigurationBuilder password(String password) {
		this.redisConfiguration.setPassword(password);
		return this;
	}
	
	public RedisConfiguration build() {
		
		if(this.redisConfiguration.getHost() == null) {
			throw new IllegalArgumentException("Host parameter should be provided.");
		}
		
		Jedis jedis = new Jedis(this.redisConfiguration.getHost(), this.redisConfiguration.getPort());
		
		if(this.redisConfiguration.getPassword() != null) {
			String status = jedis.auth(this.redisConfiguration.getPassword());
			
			if(!"OK".equalsIgnoreCase(status)) {
				throw new IllegalStateException("Password is not valid and Redis access cannot be accept commands.");
			}
			
		}
		
		this.redisConfiguration.setJedis(jedis);
		return redisConfiguration;
	}
	
}
