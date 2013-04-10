package com.lordofthejars.nosqlunit.redis.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static com.lordofthejars.nosqlunit.redis.ManagedRedisLifecycleManagerBuilder.newManagedRedis;
import static com.lordofthejars.nosqlunit.redis.replication.ReplicationGroupBuilder.master;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.redis.replication.ReplicationManagedRedis;

public class WhenInsertingDatasetWithReplicationRedis26 {

	@ClassRule
	public static ReplicationManagedRedis replication = master(
																newManagedRedis()
																.redisPath("/opt/redis-2.6.12")
																.targetPath("target/redism")
																.configurationPath(getConfigurationFilePath("src/test/resources/redis_6379.conf"))
																.port(6379)
																.build()
															   )
													    .slave(
													    		newManagedRedis()
																.redisPath("/opt/redis-2.6.12")
																.targetPath("target/rediss1")
																.configurationPath(getConfigurationFilePath("src/test/resources/redis_6380.conf"))
																.port(6380)
																.slaveOf("127.0.0.1", 6379)
																.build())
														.get();

	
	@Test
	public void insert_operation_should_be_propagated_from_master_to_slave() throws InterruptedException {
		
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		jedis.set("mykey", "myvalue");
		
		TimeUnit.SECONDS.sleep(5);
		
		Jedis jedisSlave = new Jedis("127.0.0.1", 6380);
		String value = jedisSlave.get("mykey");
		assertThat(value, is("myvalue"));
		
	}
	
	
	private static String getConfigurationFilePath(String fileName) {
		File configurationFile = new File(fileName);
		return configurationFile.getAbsolutePath();
	}
	
}
