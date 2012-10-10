package com.lordofthejars.nosqlunit.redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Sharded;


public class ShardedRedisConfigurationBuilder {

	private List<ShardInfoBuilder> shardInfoBuilders = new ArrayList<ShardedRedisConfigurationBuilder.ShardInfoBuilder>();
	
	private ShardedRedisConfiguration redisConfiguration;
	
	private ShardedRedisConfigurationBuilder() {
		redisConfiguration = new ShardedRedisConfiguration();
	}
	
	public static ShardedRedisConfigurationBuilder newShardedRedisConfiguration() {
		return new ShardedRedisConfigurationBuilder();
	}
	
	public static String host(String host) {
		return host;
	}
	
	public static int port(int port) {
		return port;
	}
	
	public ShardInfoBuilder shard(String host, int port) {
		ShardInfoBuilder child = new ShardInfoBuilder(this);
		shardInfoBuilders.add(child);
		child.host = host;
		child.port = port;
		
		return child;
	}
	
	public ShardedRedisConfiguration build() {
		
		List<JedisShardInfo> jedisShardInfos = new ArrayList<JedisShardInfo>();
		for (ShardInfoBuilder shardInfoBuilder : shardInfoBuilders) {
			jedisShardInfos.add(shardInfoBuilder.getContent());
		}
		
		ShardedRedisOperation shardedRedisOperation = new ShardedRedisOperation(new ShardedJedis(jedisShardInfos));
		redisConfiguration.setDatabaseOperation(shardedRedisOperation);
		
		return this.redisConfiguration;
	}
	
	public ShardedRedisConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.redisConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}

	public class ShardInfoBuilder {
		
		private ShardedRedisConfigurationBuilder parent;
		
		private static final int DEFAULT_TIMEOUT_IN_MILLIS = 2000;
		
		private String host;
		private int port;
		private String password;
		private int timeout = DEFAULT_TIMEOUT_IN_MILLIS;
		private int weight = Sharded.DEFAULT_WEIGHT;
		
		private ShardInfoBuilder(ShardedRedisConfigurationBuilder parent) {
			this.parent = parent;
		}
		
		public ShardInfoBuilder password(String password) {
			this.password = password;
			return this;
		}
		
		public ShardInfoBuilder timeout(int timeout) {
			this.timeout = timeout;
			return this;
		}
		
		public ShardInfoBuilder weight(int weight) {
			this.weight = weight;
			return this;
		}
		
		public ShardInfoBuilder shard(String host, int port) {
			return parent.shard(host, port);
		}
		
		public ShardedRedisConfiguration build() {
			return parent.build();
		}
		
		private JedisShardInfo getContent() {
			JedisShardInfo jedisShardInfo =  new JedisShardInfo(host, port, timeout, weight);
			jedisShardInfo.setPassword(password);
			
			return jedisShardInfo;
		}
	}
	
}
