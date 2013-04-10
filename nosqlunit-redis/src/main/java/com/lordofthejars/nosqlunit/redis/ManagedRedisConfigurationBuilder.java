package com.lordofthejars.nosqlunit.redis;


import redis.clients.jedis.Jedis;


public class ManagedRedisConfigurationBuilder {

	private static final String LOCALHOST = "127.0.0.1";
	private static final int DEFAULT_PORT = ManagedRedisLifecycleManager.DEFAULT_PORT;
	
	private RedisConfiguration redisConfiguration;
	
	private ManagedRedisConfigurationBuilder() {
		redisConfiguration = new RedisConfiguration();
		redisConfiguration.setHost(LOCALHOST);
		redisConfiguration.setPort(DEFAULT_PORT);
	}
	
	public static ManagedRedisConfigurationBuilder newManagedRedisConfiguration() {
		return new ManagedRedisConfigurationBuilder();
	}
	
	public ManagedRedisConfigurationBuilder port(int port) {
		this.redisConfiguration.setPort(port);
		return this;
	}
	
	public ManagedRedisConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.redisConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public ManagedRedisConfigurationBuilder password(String password) {
		this.redisConfiguration.setPassword(password);
		return this;
	}
	
	public RedisConfiguration build() {
		
		Jedis jedis = new Jedis(this.redisConfiguration.getHost(), this.redisConfiguration.getPort());
		
		if(this.redisConfiguration.getPassword() != null) {
			String status = jedis.auth(this.redisConfiguration.getPassword());
			
			if(!"OK".equalsIgnoreCase(status)) {
				throw new IllegalStateException("Password is not valid and Redis access cannot be accept commands.");
			}
			
		}
		this.redisConfiguration.setDatabaseOperation(new RedisOperation(jedis));
		return redisConfiguration;
	}
	
}
