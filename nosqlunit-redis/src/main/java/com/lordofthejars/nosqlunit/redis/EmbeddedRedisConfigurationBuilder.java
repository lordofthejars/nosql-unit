package com.lordofthejars.nosqlunit.redis;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.FailureHandler;


public class EmbeddedRedisConfigurationBuilder {

	private RedisConfiguration redisConfiguration;
	
	private EmbeddedRedisConfigurationBuilder() {
		redisConfiguration = new RedisConfiguration();
	}
	
	public static EmbeddedRedisConfigurationBuilder newEmbeddedRedisConfiguration() {
		return new EmbeddedRedisConfigurationBuilder();
	}
	
	public EmbeddedRedisConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.redisConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public RedisConfiguration build() {
		Jedis jedis = EmbeddedRedisInstances.getInstance().getDefaultJedis();
				
		if(jedis == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedRedis rule during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		this.redisConfiguration.setDatabaseOperation(new RedisOperation(jedis));
		return this.redisConfiguration;
	}
	
	public RedisConfiguration buildFromTargetPath(String targetPath) {
		Jedis jedis = EmbeddedRedisInstances.getInstance().getJedisByTargetPath(targetPath);
		
		if(jedis == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedRedis rule with %s target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.", targetPath);
		}
		
		this.redisConfiguration.setDatabaseOperation(new RedisOperation(jedis));
		return this.redisConfiguration;
	}
	
}
