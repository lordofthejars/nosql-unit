package com.lordofthejars.nosqlunit.infinispan;

import java.io.InputStream;
import java.util.Map;

import com.lordofthejars.nosqlunit.objects.KeyValueObjectMapper;

public class DefaultInfinispanInsertationStrategy implements InfinispanInsertationStrategy {

	@Override
	public void insert(InfinispanConnectionCallback connection, InputStream dataset) throws Throwable {
		Map<Object, Object> values = loadMap(dataset);
		connection.basicCache().putAll(values);
	}

	private Map<Object, Object> loadMap(InputStream dataScript) {
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		Map<Object, Object> values = keyValueObjectMapper.readValues(dataScript);
		return values;
	}
	
}
