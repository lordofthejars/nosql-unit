package com.lordofthejars.nosqlunit.demo.custom;

import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import redis.clients.jedis.BinaryJedisCommands;

import com.lordofthejars.nosqlunit.redis.RedisConnectionCallback;
import com.lordofthejars.nosqlunit.redis.RedisInsertationStrategy;

public class PropertiesCustomInsertation implements RedisInsertationStrategy {

	@Override
	public void insert(RedisConnectionCallback connection, InputStream dataset) throws Throwable {
		Properties properties = new Properties();
		properties.load(dataset);

		BinaryJedisCommands insertationJedis = connection.insertationJedis();
		
		Set<Entry<Object, Object>> entrySet = properties.entrySet();
		
		for (Entry<Object, Object> entry : entrySet) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			insertationJedis.set(key.getBytes(), value.getBytes());
		}
		
	}

}
