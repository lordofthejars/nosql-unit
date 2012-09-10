package com.lordofthejars.nosqlunit.redis.integration;

import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.redis.ManagedRedis;
import com.lordofthejars.nosqlunit.redis.RedisOperation;
import com.lordofthejars.nosqlunit.redis.ShardedRedisOperation;

public class WhenComparingRedisDatasetWithShards {


	private static final String INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				},\n" + 
			"				{\n" + 
			"					\"key\":\"key2\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	private static final String INSERT_SIMPLE_DATA_WITH_TWO_DIFFERENT_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				},\n" + 
			"				{\n" + 
			"					\"key\":\"key3\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	

	@ClassRule
	public static ManagedRedis managedRedis79 = newManagedRedisRule().redisPath("/opt/redis-2.4.16").targetPath("target/redis1")
			.configurationPath(getConfigurationFilePath("src/test/resources/redis_6379.conf")).port(6379).build();

	@ClassRule
	public static ManagedRedis managedRedis80 = newManagedRedisRule().redisPath("/opt/redis-2.4.16").targetPath("target/redis2")
			.configurationPath(getConfigurationFilePath("src/test/resources/redis_6380.conf")).port(6380).build();


	@Test
	public void no_exception_should_be_thrown_if_content_is_expected() {

		ShardedJedis jedis = getShardedConnection();

		ShardedRedisOperation shardedRedisOperation = new ShardedRedisOperation(jedis);
		shardedRedisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS.getBytes()));

		boolean isExpectedData = shardedRedisOperation.databaseIs(new ByteArrayInputStream(
				INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS.getBytes()));
		assertThat(isExpectedData, is(true));

	}

	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_simple_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_TWO_DIFFERENT_ELEMENTS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key3 is not found."));
		}
		
	}
	
	private ShardedJedis getShardedConnection() {
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		JedisShardInfo si = new JedisShardInfo("127.0.0.1", 6379);
		shards.add(si);
		si = new JedisShardInfo("127.0.0.1", 6380);
		shards.add(si);

		ShardedJedis jedis = new ShardedJedis(shards);
		return jedis;
	}

	private static String getConfigurationFilePath(String fileName) {
		File configurationFile = new File(fileName);
		return configurationFile.getAbsolutePath();
	}

}
