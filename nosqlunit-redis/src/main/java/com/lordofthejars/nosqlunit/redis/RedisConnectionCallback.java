package com.lordofthejars.nosqlunit.redis;

import java.util.Collection;

import redis.clients.jedis.Jedis;

public interface RedisConnectionCallback {

	Jedis getActiveJedis(byte[] key);
	
	Collection<Jedis> getAllJedis();
	
}
