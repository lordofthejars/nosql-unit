package com.lordofthejars.nosqlunit.mongodb;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fakemongo.Fongo;
import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.mongodb.DBPort;
import com.mongodb.Mongo;

public class InMemoryMongoDbLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMongoDb.class); 
	private static final String LOCALHOST = "127.0.0.1";
	private static final int PORT = DBPort.PORT;
	
	public static final String INMEMORY_MONGO_TARGET_PATH = "target" + File.separatorChar + "mongo-data"
			+ File.separatorChar + "impermanent-db";
	
	private String targetPath = INMEMORY_MONGO_TARGET_PATH;
	
	@Override
	public String getHost() {
		return LOCALHOST+targetPath;
	}

	@Override
	public int getPort() {
		return PORT;
	}

	@Override
	public void doStart() throws Throwable {

		LOGGER.info("Starting EmbeddedInMemory MongoDb instance.");
		EmbeddedMongoInstancesFactory.getInstance().addEmbeddedInstance(fongo(targetPath), targetPath);
		LOGGER.info("Started EmbeddedInMemory MongoDb instance.");

	}

	private Mongo fongo(String targetPath) {
		Fongo fongo = new Fongo(targetPath);
		return fongo.getMongo();
	}
	
	@Override
	public void doStop() {
		
		LOGGER.info("Stopping EmbeddedInMemory MongoDb instance.");
		
		EmbeddedMongoInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
		
		LOGGER.info("Stopped EmbeddedInMemory MongoDb instance.");
		
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public String getTargetPath() {
		return targetPath;
	}
	
}
