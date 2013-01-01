package com.lordofthejars.nosqlunit.core.integration;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.InsertationStrategy;

public class MyCustomInsertation implements InsertationStrategy<Object> {
	
	@Override
	public void insert(Object connection, InputStream dataset) throws Throwable {
		
	}
	
}