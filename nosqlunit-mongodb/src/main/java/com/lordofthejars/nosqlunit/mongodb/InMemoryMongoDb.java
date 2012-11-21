package com.lordofthejars.nosqlunit.mongodb;

import org.junit.rules.ExternalResource;

public class InMemoryMongoDb extends ExternalResource {

	private InMemoryMongoDbLifecycleManager inMemoryMongoDbLifecycleManager = new InMemoryMongoDbLifecycleManager();

	@Override
	public void before() throws Throwable {
		inMemoryMongoDbLifecycleManager.startEngine();
	}

	@Override
	public void after() {
		inMemoryMongoDbLifecycleManager.stopEngine();
	}
	

}
