package com.lordofthejars.nosqlunit.core.integration;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.ComparisonStrategy;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class MyCustomComparision implements ComparisonStrategy<Object>{

	@Override
	public boolean compare(Object connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		return false;
	}

}
