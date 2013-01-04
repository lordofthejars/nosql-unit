package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public interface InsertionStrategy<S> {

	void insert(S connection, InputStream dataset) throws Throwable;
	
}
