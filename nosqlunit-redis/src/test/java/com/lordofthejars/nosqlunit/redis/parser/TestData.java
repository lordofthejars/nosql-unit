package com.lordofthejars.nosqlunit.redis.parser;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.redis.parser.DataReader;

public class TestData {

	
	@Test
	public void load() throws FileNotFoundException {
		
		Map<byte[], byte[]> elements = new HashMap<byte[], byte[]>();
		
		byte[] bytes = "key".getBytes();
		byte[] bytes2 = new byte[]{107, 101, 121};
		
		elements.put(bytes, "mm".getBytes());
		
		
		System.out.println(elements.containsKey("key".getBytes()));
		
	}
	
}
