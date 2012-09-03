package com.lordofthejars.nosqlunit.redis;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import static com.lordofthejars.nosqlunit.redis.RemoteRedisConfigurationBuilder.newRemoteRedisConfiguration;
import org.junit.Test;

public class WhenRemoteRedisConfigurationIsRequired {

	@Test
	public void remote_configuration_redis_should_contain_remote_parameters() {
		
		RedisConfiguration remoteConfiguration = newRemoteRedisConfiguration().host("localhost").build();
		
		assertThat(remoteConfiguration.getHost(), is("localhost"));
		assertThat(remoteConfiguration.getPort(), is(ManagedRedis.DEFAULT_PORT));
		assertThat(remoteConfiguration.getJedis(), notNullValue());
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void remote_configuration_redis_should_throw_an_exception_if_no_host_provided() {
		
		RedisConfiguration remoteConfiguration = newRemoteRedisConfiguration().build();
		
	}
	
}
