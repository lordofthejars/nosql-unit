package com.lordofthejars.nosqlunit.redis;

import static com.lordofthejars.nosqlunit.redis.ManagedRedisConfigurationBuilder.newManagedRedisConfiguration;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.junit.Test;

public class WhenDefaultRedisRuleConfigurationIsCreated {

	@Test
	public void configuration_object_should_contain_default_values() {
		
		RedisConfiguration redisConfiguration = newManagedRedisConfiguration().build();
		
		assertThat(redisConfiguration.getHost(), is("127.0.0.1"));
		assertThat(redisConfiguration.getPort(), is(ManagedRedis.DEFAULT_PORT));
		assertThat(redisConfiguration.getDatabaseOperation(), notNullValue());
		
	}
	
}
