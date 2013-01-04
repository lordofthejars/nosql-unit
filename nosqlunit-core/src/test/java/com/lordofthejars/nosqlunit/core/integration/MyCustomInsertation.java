package com.lordofthejars.nosqlunit.core.integration;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.InsertionStrategy;

public class MyCustomInsertation implements InsertionStrategy<Object> {
	
	@Override
	public void insert(Object connection, InputStream dataset) throws Throwable {
		
	}
	
}