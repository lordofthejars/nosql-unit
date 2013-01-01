package com.lordofthejars.nosqlunit.redis;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class DefaultRedisComparisionStrategy implements RedisComparisionStrategy {

	@Override
	public boolean compare(RedisConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
			Throwable {
		RedisAssertion.strictAssertEquals(connection, dataset);
		return true;
	}

}
