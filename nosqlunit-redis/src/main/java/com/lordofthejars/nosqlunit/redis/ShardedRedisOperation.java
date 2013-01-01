package com.lordofthejars.nosqlunit.redis;

import static ch.lambdaj.Lambda.forEach;

import java.io.InputStream;
import java.util.Collection;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class ShardedRedisOperation extends AbstractCustomizableDatabaseOperation<RedisConnectionCallback, ShardedJedis> {

	private ShardedJedis shardedJedis;
	
	public ShardedRedisOperation(ShardedJedis shardedJedis) {
		this.shardedJedis = shardedJedis;
		setInsertationStrategy(new DefaultRedisInsertationStrategy());
		setComparisionStrategy(new DefaultRedisComparisionStrategy());
	}
	
	@Override
	public void insert(InputStream dataScript) {
		insertData(dataScript);
	}

	private void insertData(InputStream dataScript) {
		try {
			executeInsertation(new RedisConnectionCallback() {
				
				@Override
				public Collection<Jedis> getAllJedis() {
					return shardedJedis.getAllShards();
				}
				
				@Override
				public Jedis getActiveJedis(byte[] key) {
					return shardedJedis.getShard(key);
				}

				@Override
				public BinaryJedisCommands insertationJedis() {
					return shardedJedis;
				}
			}, dataScript);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void deleteAll() {
		forEach(shardedJedis.getAllShards()).flushAll();
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		return compareData(expectedData);
	}

	private boolean compareData(InputStream expectedData) throws NoSqlAssertionError {
		try {
			return executeComparision(new RedisConnectionCallback() {
				
				@Override
				public Collection<Jedis> getAllJedis() {
					return shardedJedis.getAllShards();
				}
				
				@Override
				public Jedis getActiveJedis(byte[] key) {
					return shardedJedis.getShard(key);
				}

				@Override
				public BinaryJedisCommands insertationJedis() {
					return shardedJedis;
				}
			}, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public ShardedJedis connectionManager() {
		return shardedJedis;
	}

}
