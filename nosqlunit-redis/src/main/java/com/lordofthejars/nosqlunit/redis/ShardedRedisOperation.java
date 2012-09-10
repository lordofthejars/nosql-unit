package com.lordofthejars.nosqlunit.redis;

import static ch.lambdaj.Lambda.forEach;

import java.io.InputStream;
import java.util.Collection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.redis.parser.DataReader;

public class ShardedRedisOperation implements DatabaseOperation<ShardedJedis> {

	private ShardedJedis shardedJedis;
	private DataReader dataReader;
	
	public ShardedRedisOperation(ShardedJedis shardedJedis) {
		this.shardedJedis = shardedJedis;
		this.dataReader = new DataReader(shardedJedis);
	}
	
	@Override
	public void insert(InputStream dataScript) {
		this.dataReader.read(dataScript);
	}

	@Override
	public void deleteAll() {
		forEach(shardedJedis.getAllShards()).flushAll();
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		RedisAssertion.strictAssertEquals(new RedisConnectionCallback() {
			
			@Override
			public Collection<Jedis> getAllJedis() {
				return shardedJedis.getAllShards();
			}
			
			@Override
			public Jedis getActiveJedis(byte[] key) {
				return shardedJedis.getShard(key);
			}
		}, expectedData);
		return true;
	}

	@Override
	public ShardedJedis connectionManager() {
		return shardedJedis;
	}

}
