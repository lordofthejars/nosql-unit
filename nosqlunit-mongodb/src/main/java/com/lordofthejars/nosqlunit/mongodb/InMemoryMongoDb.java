package com.lordofthejars.nosqlunit.mongodb;

import jmockmongo.MockMongo;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMongoDb extends ExternalResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMongoDb.class); 
	
	private MockMongo mockMongo = new MockMongo();

	@Override
	protected void before() throws Throwable {
		
		LOGGER.info("Starting EmbeddedInMemory MongoDb instance.");
		
		mockMongo.start();
		
		LOGGER.info("Started EmbeddedInMemory MongoDb instance.");
	}

	@Override
	protected void after() {
		LOGGER.info("Stopping EmbeddedInMemory MongoDb instance.");
		
		mockMongo.stop();
		
		LOGGER.info("Stopped EmbeddedInMemory MongoDb instance.");
	}
	

}
