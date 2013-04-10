package com.lordofthejars.nosqlunit.demo.redis;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.redis.ManagedRedisConfigurationBuilder.newManagedRedisConfiguration;
import static com.lordofthejars.nosqlunit.redis.ManagedRedisLifecycleManagerBuilder.newManagedRedis;
import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;
import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule;
import static com.lordofthejars.nosqlunit.redis.replication.ReplicationGroupBuilder.master;

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
import com.lordofthejars.nosqlunit.redis.replication.ReplicationManagedRedis;

public class WhenMasterSlaveReplicationIsConfigured {

	private static final String MASTER_CONFIGURATION_DIRECTORY = 
			"src/test/resources/com/lordofthejars/nosqlunit/demo/redis/master-redis.conf";
	private static final String SLAVE_CONFIGURATION_DIRECTORY = 
			"src/test/resources/com/lordofthejars/nosqlunit/demo/redis/slave-redis.conf";

	@ClassRule
	public static ReplicationManagedRedis replication = master(
																newManagedRedis()
																.redisPath("/opt/redis-2.6.12")
																.targetPath("target/redism")
																.configurationPath(getConfigurationFilePath(MASTER_CONFIGURATION_DIRECTORY))
																.port(6379)
																.build()
															   )
													    .slave(
													    		newManagedRedis()
																.redisPath("/opt/redis-2.6.12")
																.targetPath("target/rediss1")
																.configurationPath(getConfigurationFilePath(SLAVE_CONFIGURATION_DIRECTORY))
																.port(6380)
																.slaveOf("127.0.0.1", 6379)
																.build())
														.get();
	

	@Rule
	public RedisRule masterRedisRule = newRedisRule().configure(
			newManagedRedisConfiguration().connectionIdentifier("master").port(6379).build()).build();


	@Inject
	@Named("master")
	private Jedis masterConnection;
	
	
	@Test
	@UsingDataSet(withSelectiveLocations = { @Selective(identifier = "master", locations = "book.json") }, loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void should_replicate_data_into_slave_node() throws InterruptedException {

		String theHobbitTitle = masterConnection.hget("The Hobbit", "title");
		assertThat(theHobbitTitle, is("The Hobbit"));
		
		TimeUnit.SECONDS.sleep(5);
		
		Jedis slaveConnection = new Jedis("localhost", 6380);
		
		theHobbitTitle = slaveConnection.hget("The Hobbit", "title");
		assertThat(theHobbitTitle, is("The Hobbit"));
	
	}

	private static String getConfigurationFilePath(String fileName) {
		File configurationFile = new File(fileName);
		return configurationFile.getAbsolutePath();
	}
	
}
