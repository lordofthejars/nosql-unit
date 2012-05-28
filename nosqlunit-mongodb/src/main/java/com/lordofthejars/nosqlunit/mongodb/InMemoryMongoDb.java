package com.lordofthejars.nosqlunit.mongodb;

import jmockmongo.MockMongo;

import org.junit.rules.ExternalResource;

public class InMemoryMongoDb extends ExternalResource {

	private MockMongo mockMongo = new MockMongo();

	@Override
	protected void before() throws Throwable {
		mockMongo.start();
	}

	@Override
	protected void after() {
		mockMongo.stop();
	}
	

}
