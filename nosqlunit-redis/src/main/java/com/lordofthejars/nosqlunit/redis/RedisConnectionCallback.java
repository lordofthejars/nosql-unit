package com.lordofthejars.nosqlunit.redis;

import java.util.Collection;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;

public interface RedisConnectionCallback {

	BinaryJedisCommands insertationJedis();
	
	Jedis getActiveJedis(byte[] key);
	
	Collection<Jedis> getAllJedis();
	
}
