package com.lordofthejars.nosqlunit.redis;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedisConfigurationBuilder.newEmbeddedRedisConfiguration;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;

public class WhenEmbeddedConfigurationIsRequired {

	@Test
	public void in_memory_configuration_should_use_default_embedded_instance() {
		
		Jedis jedis = mock(Jedis.class);
		EmbeddedRedisInstances.getInstance().addJedis(jedis, "a");
		
		EmbeddedRedisConfigurationBuilder embeddedRedisConfiguration = newEmbeddedRedisConfiguration();
		RedisConfiguration embeddedConfiguration = embeddedRedisConfiguration.build();
		EmbeddedRedisInstances.getInstance().removeJedis("a");
		
		assertThat(embeddedConfiguration.getDatabaseOperation().connectionManager(), is((BinaryJedisCommands)jedis));
		
	}

	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_default_embedded() {
		
		
		EmbeddedRedisConfigurationBuilder embeddedRedisConfiguration = newEmbeddedRedisConfiguration();
		embeddedRedisConfiguration.build();
		
	}

	@Test
	public void in_memory_configuration_should_use_targeted_instance() {
		
		Jedis jedis1 = mock(Jedis.class);
		Jedis jedis2 = mock(Jedis.class);
		EmbeddedRedisInstances.getInstance().addJedis(jedis1, "a");
		EmbeddedRedisInstances.getInstance().addJedis(jedis2, "b");
		
		EmbeddedRedisConfigurationBuilder embeddedRedisConfiguration = newEmbeddedRedisConfiguration();
		RedisConfiguration embeddedConfiguration = embeddedRedisConfiguration.buildFromTargetPath("a");
		EmbeddedRedisInstances.getInstance().removeJedis("a");
		EmbeddedRedisInstances.getInstance().removeJedis("b");
		
		assertThat(embeddedConfiguration.getDatabaseOperation().connectionManager(), is((BinaryJedisCommands)jedis1));
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_targeted_instance() {
		EmbeddedRedisConfigurationBuilder embeddedRedisConfiguration = newEmbeddedRedisConfiguration();
		embeddedRedisConfiguration.buildFromTargetPath("a");
		
	}
	
}
