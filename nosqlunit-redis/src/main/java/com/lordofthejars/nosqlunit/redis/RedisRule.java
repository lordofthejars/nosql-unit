package com.lordofthejars.nosqlunit.redis;

import static com.lordofthejars.nosqlunit.redis.ManagedRedisConfigurationBuilder.newManagedRedisConfiguration;
import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class RedisRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<Jedis> databaseOperation;
	
	public static class RedisRuleBuilder {
		
		private RedisConfiguration redisConfiguration;
		private Object target;
		
		private RedisRuleBuilder() {
			super();
		}
		
		public static RedisRuleBuilder newRedisRule() {
			return new RedisRuleBuilder();
		}
		
		public RedisRuleBuilder configure(RedisConfiguration redisConfiguration) {
			this.redisConfiguration = redisConfiguration;
			return this;
		}
		
		public RedisRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public RedisRule defaultManagedRedis() {
			return new RedisRule(newManagedRedisConfiguration().build());
		}
		
		public RedisRule defaultManagedRedis(Object target) {
			return new RedisRule(newManagedRedisConfiguration().build(), target);
		}
		
		public RedisRule build() {

			if(this.redisConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new RedisRule(redisConfiguration, target);
		}
		
	}
	
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
