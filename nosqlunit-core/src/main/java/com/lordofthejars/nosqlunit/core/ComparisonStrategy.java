package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public interface ComparisonStrategy<S> {

	boolean compare(S connection, InputStream dataset) throws NoSqlAssertionError, Throwable;
	void setIgnoreProperties(String[] ignoreProperties);
}
