package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public interface ComparisionStrategy<S> {

	boolean compare(S connection, InputStream dataset) throws NoSqlAssertionError, Throwable;
	
}
