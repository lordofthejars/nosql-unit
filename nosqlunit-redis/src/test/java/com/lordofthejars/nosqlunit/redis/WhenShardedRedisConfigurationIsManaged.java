package com.lordofthejars.nosqlunit.redis;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static com.lordofthejars.nosqlunit.redis.ShardedRedisConfigurationBuilder.host;
import static com.lordofthejars.nosqlunit.redis.ShardedRedisConfigurationBuilder.newShardedRedisConfiguration;
import static com.lordofthejars.nosqlunit.redis.ShardedRedisConfigurationBuilder.port;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Collection;

import org.junit.Test;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Sharded;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class WhenShardedRedisConfigurationIsManaged {

	@Test
	public void only_host_and_port_should_be_mandatory() {

		ShardedRedisConfiguration shardedRedisConfiguration = newShardedRedisConfiguration()
				.shard(host("127.0.0.1"), port(ManagedRedis.DEFAULT_PORT))
				.shard(host("127.0.0.1"), port(ManagedRedis.DEFAULT_PORT + 1)).build();
		
		DatabaseOperation<? extends BinaryJedisCommands> databaseOperation = shardedRedisConfiguration.getDatabaseOperation();
		assertThat(databaseOperation, instanceOf(ShardedRedisOperation.class));
		
		ShardedRedisOperation shardedRedisOperation = (ShardedRedisOperation)databaseOperation;
		
		ShardedJedis connectionManager = shardedRedisOperation.connectionManager();
		Collection<JedisShardInfo> allShardInfo = connectionManager.getAllShardInfo();
		
		JedisShardInfo localhostFirstShard = selectFirst(allShardInfo, having(on(JedisShardInfo.class).getPort(), is(ManagedRedis.DEFAULT_PORT)));
		
		assertThat(localhostFirstShard, notNullValue());
		assertThat(localhostFirstShard.getHost(), is("127.0.0.1"));
		assertThat(localhostFirstShard.getName(), nullValue());
		assertThat(localhostFirstShard.getPassword(), nullValue());
		assertThat(localhostFirstShard.getTimeout(), is(2000));
		assertThat(localhostFirstShard.getWeight(), is(Sharded.DEFAULT_WEIGHT));
		
		JedisShardInfo localhostSecondShard = selectFirst(allShardInfo, having(on(JedisShardInfo.class).getPort(), is(ManagedRedis.DEFAULT_PORT+1)));
		
		assertThat(localhostSecondShard, notNullValue());
		assertThat(localhostSecondShard.getHost(), is("127.0.0.1"));
		assertThat(localhostSecondShard.getName(), nullValue());
		assertThat(localhostSecondShard.getPassword(), nullValue());
		assertThat(localhostSecondShard.getTimeout(), is(2000));
		assertThat(localhostSecondShard.getWeight(), is(Sharded.DEFAULT_WEIGHT));
	}

	@Test
	public void all_configuration_parameters_should_be_configurable() {
		
		ShardedRedisConfiguration shardedRedisConfiguration = newShardedRedisConfiguration()
				.shard(host("127.0.0.1"), port(ManagedRedis.DEFAULT_PORT))
					.password("a")
					.timeout(1000)
					.weight(1000)
				.shard(host("127.0.0.1"), port(ManagedRedis.DEFAULT_PORT + 1))
					.password("b")
					.timeout(3000)
					.weight(3000)
				.build();
		
		DatabaseOperation<? extends BinaryJedisCommands> databaseOperation = shardedRedisConfiguration.getDatabaseOperation();
		assertThat(databaseOperation, instanceOf(ShardedRedisOperation.class));
		
		ShardedRedisOperation shardedRedisOperation = (ShardedRedisOperation)databaseOperation;
		
		ShardedJedis connectionManager = shardedRedisOperation.connectionManager();
		Collection<JedisShardInfo> allShardInfo = connectionManager.getAllShardInfo();
		
		JedisShardInfo localhostFirstShard = selectFirst(allShardInfo, having(on(JedisShardInfo.class).getPort(), is(ManagedRedis.DEFAULT_PORT)));
		
		assertThat(localhostFirstShard, notNullValue());
		assertThat(localhostFirstShard.getHost(), is("127.0.0.1"));
		assertThat(localhostFirstShard.getPassword(), is("a"));
		assertThat(localhostFirstShard.getTimeout(), is(1000));
		assertThat(localhostFirstShard.getWeight(), is(1000));
	
		JedisShardInfo localhostSecondShard = selectFirst(allShardInfo, having(on(JedisShardInfo.class).getPort(), is(ManagedRedis.DEFAULT_PORT+1)));
		
		assertThat(localhostSecondShard, notNullValue());
		assertThat(localhostSecondShard.getHost(), is("127.0.0.1"));
		assertThat(localhostSecondShard.getPassword(), is("b"));
		assertThat(localhostSecondShard.getTimeout(), is(3000));
		assertThat(localhostSecondShard.getWeight(), is(3000));
		
	}
	
}
