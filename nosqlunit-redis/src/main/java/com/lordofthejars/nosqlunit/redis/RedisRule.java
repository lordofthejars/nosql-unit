package com.lordofthejars.nosqlunit.redis;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class RedisRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<Jedis> databaseOperation;
	
	public RedisRule(RedisConfiguration redisConfiguration) {
		super(redisConfiguration.getConnectionIdentifier());
		
		this.databaseOperation = new RedisOperation(redisConfiguration.getJedis());
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public RedisRule(RedisConfiguration redisConfiguration, Object target) {
		this(redisConfiguration);
		setTarget(target);
	}
	
	@Override
	public DatabaseOperation<Jedis> getDatabaseOperation() {
		return databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
