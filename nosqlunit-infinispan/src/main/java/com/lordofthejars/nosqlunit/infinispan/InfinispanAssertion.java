package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.bool.Bool.is;
import static com.lordofthejars.bool.Bool.the;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.api.BasicCache;

import com.lordofthejars.nosqlunit.core.FailureHandler;

public class InfinispanAssertion {

	public static void strictAssertEquals(BasicCache<Object, Object> cache, Map<Object, Object> expectedMap) {

		int expectedNumberOfKeys = expectedMap.size();
		int currentNumberOfKeys = cache.size();

		checkNumberOfElements(expectedNumberOfKeys, currentNumberOfKeys);
		checkElements(cache, expectedMap);
	}

	private static void checkElements(BasicCache<Object, Object> cache, Map<Object, Object> expectedMap) throws Error {
		
		Set<Entry<Object, Object>> expectedElements = expectedMap.entrySet();

		for (Entry<Object, Object> expectedElement : expectedElements) {
			checkElement(cache, expectedElement);
		}
	}

	private static void checkElement(BasicCache<Object, Object> cache, Entry<Object, Object> expectedElement) throws Error {
		Object expectedKey = expectedElement.getKey();

		if (cache.containsKey(expectedKey)) {
			Object currentValue = cache.get(expectedKey);
			Object expectedValue = expectedElement.getValue();

			if (the(currentValue, is(not(equalTo(expectedValue))))) {
				throw FailureHandler.createFailure("Object for key %s should be %s but was found %s.", expectedKey,
						expectedValue, currentValue);
			}

		} else {
			throw FailureHandler.createFailure("Key %s was not found.", expectedKey);
		}
	}

	private static void checkNumberOfElements(int expectedNumberOfKeys, int currentNumberOfKeys) throws Error {
		if (expectedNumberOfKeys != currentNumberOfKeys) {
			throw FailureHandler.createFailure("Number of expected keys are %s but was found %s.",
					expectedNumberOfKeys, currentNumberOfKeys);
		}
	}

}
