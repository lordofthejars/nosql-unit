package com.lordofthejars.nosqlunit.redis;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder.newEmbeddedRedisRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;

public class WhenEmbeddedRedisLifecycleIsManaged {
	
	private static final String DEFAULT_JEDIS_TARGET_PATH = EmbeddedRedis.INMEMORY_REDIS_TARGET_PATH;
	private static final int PORT = ManagedRedis.DEFAULT_PORT;
	private static final String LOCALHOST = "127.0.0.1";
	
	
	@Test
	public void redis_should_start_in_memory_and_working() throws Throwable {
		
		EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_JEDIS_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedRedisInstances.getInstance().getJedisByTargetPath(DEFAULT_JEDIS_TARGET_PATH), notNullValue());
			}
		};
		
		Statement decotedStatement = embeddedRedis.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_JEDIS_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedRedisInstances.getInstance().getJedisByTargetPath(DEFAULT_JEDIS_TARGET_PATH), nullValue());
		
	}
	
	@Test
	public void simulataneous_redis_should_start_only_one_instance_for_location() throws Throwable {

		EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				EmbeddedRedis defaultEmbeddedRedis = newEmbeddedRedisRule().build();
				
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_JEDIS_TARGET_PATH, PORT), is(true));
						assertThat(EmbeddedRedisInstances.getInstance().getJedisByTargetPath(DEFAULT_JEDIS_TARGET_PATH), notNullValue());
					}
				};
				
				Statement defaultStatement = defaultEmbeddedRedis.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_JEDIS_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedRedisInstances.getInstance().getJedisByTargetPath(DEFAULT_JEDIS_TARGET_PATH), notNullValue());
				
			}
		};
		
		Statement decotedStatement = embeddedRedis.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_JEDIS_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedRedisInstances.getInstance().getJedisByTargetPath(DEFAULT_JEDIS_TARGET_PATH), nullValue());
		
		
	}
}
