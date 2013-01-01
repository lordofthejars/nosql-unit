package com.lordofthejars.nosqlunit.infinispan;

import java.io.InputStream;
import java.util.Map;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.objects.KeyValueObjectMapper;

public class DefaultInfinispanComparisionStrategy implements InfinispanComparisionStrategy {

	@Override
	public boolean compare(InfinispanConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		Map<Object, Object> expectedMap = loadMap(dataset);
		InfinispanAssertion.strictAssertEquals(connection.basicCache(), expectedMap);
		
		return true;
	}

	private Map<Object, Object> loadMap(InputStream dataScript) {
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		Map<Object, Object> values = keyValueObjectMapper.readValues(dataScript);
		return values;
	}
	
}
