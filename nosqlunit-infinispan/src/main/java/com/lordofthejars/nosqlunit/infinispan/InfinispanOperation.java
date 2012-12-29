package com.lordofthejars.nosqlunit.infinispan;

import java.io.InputStream;
import java.util.Map;

import org.infinispan.api.BasicCache;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.objects.KeyValueObjectMapper;

public class InfinispanOperation implements DatabaseOperation<BasicCache<Object, Object>> {

	private BasicCache<Object, Object> cache;
	
	
	public InfinispanOperation(BasicCache<Object, Object> cache) {
		this.cache = cache;
	}
	
	@Override
	public void insert(InputStream dataScript) {
		
		Map<Object, Object> values = loadMap(dataScript);
		this.cache.putAll(values);
		
	}

	private Map<Object, Object> loadMap(InputStream dataScript) {
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		Map<Object, Object> values = keyValueObjectMapper.readValues(dataScript);
		return values;
	}

	@Override
	public void deleteAll() {
		this.cache.clear();
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		
		Map<Object, Object> expectedMap = loadMap(expectedData);
		InfinispanAssertion.strictAssertEquals(cache, expectedMap);
		
		return true;
	}

	@Override
	public BasicCache connectionManager() {
		return this.cache;
	}

}
