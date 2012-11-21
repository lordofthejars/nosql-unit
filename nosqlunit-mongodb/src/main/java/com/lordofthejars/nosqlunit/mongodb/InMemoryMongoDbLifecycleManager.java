package com.lordofthejars.nosqlunit.mongodb;

import jmockmongo.MockMongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class InMemoryMongoDbLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMongoDb.class); 
	private static final String LOCALHOST = "127.0.0.1";
	
	private MockMongo mockMongo = new MockMongo();
	
	
	@Override
	protected String getHost() {
		return LOCALHOST;
	}

	@Override
	protected int getPort() {
		return MockMongo.DEFAULT_PORT;
	}

	@Override
	protected void doStart() throws Throwable {

		LOGGER.info("Starting EmbeddedInMemory MongoDb instance.");
		
		mockMongo.start();
		
		LOGGER.info("Started EmbeddedInMemory MongoDb instance.");

	}

	@Override
	protected void doStop() {
		
		LOGGER.info("Stopping EmbeddedInMemory MongoDb instance.");
		
		mockMongo.stop();
		
		LOGGER.info("Stopped EmbeddedInMemory MongoDb instance.");
		
	}

}
