package com.lordofthejars.nosqlunit.redis.embedded;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder.newEmbeddedRedisRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.ClassRule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.redis.EmbeddedRedis;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedisInstances;
import com.lordofthejars.nosqlunit.redis.RedisOperation;

public class WhenEmbeddedRedisOperationsAreExecuted {

	private static final String INSERT_DATA ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			},\n" + 
			"      		{\"list\": [{\n" + 
			"              			\"key\":\"key3\",\n" + 
			"              			\"values\":[\n" + 
			"                  			{\"value\":\"value3\"},\n" + 
			"                  			{\"value\":\"value4\"}\n" + 
			"              			]\n" + 
			"					  }]\n" + 
			"      		},\n" + 
			"      		\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key4\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":2, \"value\":\"value5\" },{\"score\":3, \"value\":\"1\" }, {\"score\":1, \"value\":\"value6\" }]\n" + 
			"                 }]\n" + 
			"      		},\n" +
			"			{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		},\n"+
			"			{\"set\":[{\n" + 
			"              			\"key\":\"key5\",\n" + 
			"              			\"values\":[\n" + 
			"                  			{\"value\":\"value3\"},\n" + 
			"                  			{\"value\":\"value4\"}\n" + 
			"              			]\n" + 
			"					  }]\n" + 
			"      		}\n"+
			"]\n" + 
			"}";
	
	@ClassRule
	public static EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();
	
	@Test
	public void insert_operation_should_add_all_dataset_to_redis() throws InterruptedException {
	
		Jedis jedis = EmbeddedRedisInstances.getInstance().getDefaultJedis();
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_DATA.getBytes()));
		
		String simpleValue = jedis.get("key1");
		assertThat(simpleValue, is("value1"));
		
		List<String> lrange = jedis.lrange("key3", 0, -1);
		assertThat(lrange, containsInAnyOrder("value3","value4"));
		
		Set<String> zrange = jedis.zrange("key4", 0, -1);
		assertThat(zrange, contains("value6", "value5", "1"));
		
		Map<String, String> hgetAll = jedis.hgetAll("user");
		
		assertThat(hgetAll, hasEntry("name", "alex"));
		assertThat(hgetAll, hasEntry("password", "alex"));
		
		Set<String> members = jedis.smembers("key5");
		
		assertThat(members, contains("value3", "value4"));
		
		jedis.flushAll();
		
	}
	
	@Test
	public void delete_all_operation_should_remove_all_data() {
		
		Jedis jedis = EmbeddedRedisInstances.getInstance().getDefaultJedis();
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		jedis.set("key1", "value1");
		
		redisOperation.deleteAll();
		
		assertThat(jedis.get("key1"), nullValue());
		
	}
	
}
