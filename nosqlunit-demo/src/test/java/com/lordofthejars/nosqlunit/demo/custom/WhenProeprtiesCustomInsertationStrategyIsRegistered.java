package com.lordofthejars.nosqlunit.demo.custom;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder.newEmbeddedRedisRule;
import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import javax.inject.Inject;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.annotation.CustomInsertationStrategy;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedis;
import com.lordofthejars.nosqlunit.redis.RedisRule;

@CustomInsertationStrategy(insertationStrategy=PropertiesCustomInsertation.class)
public class WhenProeprtiesCustomInsertationStrategyIsRegistered {

	@ClassRule
	public static EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();

	@Rule
	public RedisRule redisRule = newRedisRule().defaultEmbeddedRedis();
	
	@Inject
	public Jedis jedis;
	
	@Test
	@UsingDataSet(locations="data.properties", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void data_should_be_inserted_from_properties_file() {
		String name = jedis.get("name");
		String surname = jedis.get("surname");
		
		assertThat(name, is("alex"));
		assertThat(surname, is("soto"));
	}
	
}
