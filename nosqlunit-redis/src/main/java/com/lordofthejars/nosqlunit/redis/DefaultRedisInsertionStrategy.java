package com.lordofthejars.nosqlunit.redis;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.redis.parser.DataReader;

public class DefaultRedisInsertionStrategy implements RedisInsertionStrategy {

	@Override
	public void insert(RedisConnectionCallback connection, InputStream dataset) throws Throwable {
		DataReader dataReader = new DataReader(connection.insertionJedis());
		dataReader.read(dataset);
	}

}
