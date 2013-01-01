package com.lordofthejars.nosqlunit.core.integration;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.ComparisionStrategy;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class MyCustomComparision implements ComparisionStrategy<Object>{

	@Override
	public boolean compare(Object connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		return false;
	}

}
