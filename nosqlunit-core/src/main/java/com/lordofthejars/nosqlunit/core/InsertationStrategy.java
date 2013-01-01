package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public interface InsertationStrategy<S> {

	void insert(S connection, InputStream dataset) throws Throwable;
	
}
