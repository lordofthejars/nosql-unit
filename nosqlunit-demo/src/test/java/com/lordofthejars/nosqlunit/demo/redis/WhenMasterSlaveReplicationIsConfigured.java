package com.lordofthejars.nosqlunit.demo.redis;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.redis.ManagedRedisConfigurationBuilder.newManagedRedisConfiguration;
import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;
import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule;

import java.io.File;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.redis.ManagedRedis;
import com.lordofthejars.nosqlunit.redis.RedisRule;

public class WhenMasterSlaveReplicationIsConfigured {

	static {
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.17");
	}

	private static final File MASTER_CONFIGURATION_DIRECTORY = new File(
			"src/test/resources/com/lordofthejars/nosqlunit/demo/redis/master-redis.conf");
	private static final File SLAVE_CONFIGURATION_DIRECTORY = new File(
			"src/test/resources/com/lordofthejars/nosqlunit/demo/redis/slave-redis.conf");

	@ClassRule
	public static ManagedRedis masterRedis = newManagedRedisRule()
			.configurationPath(MASTER_CONFIGURATION_DIRECTORY.getAbsolutePath()).targetPath("target/redis1").port(6379).build();

	@ClassRule
	public static ManagedRedis slaveRedis = newManagedRedisRule()
			.configurationPath(SLAVE_CONFIGURATION_DIRECTORY.getAbsolutePath()).targetPath("target/redis2").port(6380).build();

	@Rule
	public RedisRule masterRedisRule = newRedisRule().configure(
			newManagedRedisConfiguration().connectionIdentifier("master").port(6379).build()).build();

	@Rule
	public RedisRule slaveRedisRule = newRedisRule().configure(
			newManagedRedisConfiguration().connectionIdentifier("slave").port(6380).slaveOf("127.0.0.1", 6379).build())
			.build();

	@Inject
	@Named("master")
	private Jedis masterConnection;
	
	@Inject
	@Named("slave")
	private Jedis slaveConnection;
	
	@Test
	@UsingDataSet(withSelectiveLocations = { @Selective(identifier = "master", locations = "book.json") }, loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void should_replicate_data_into_slave_node() throws InterruptedException {

		String theHobbitTitle = masterConnection.hget("The Hobbit", "title");
		assertThat(theHobbitTitle, is("The Hobbit"));
		
		TimeUnit.SECONDS.sleep(5);
		
		theHobbitTitle = slaveConnection.hget("The Hobbit", "title");
		assertThat(theHobbitTitle, is("The Hobbit"));
	
	}

}
