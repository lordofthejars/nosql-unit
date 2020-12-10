package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.util.EmbeddedInstances;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.ImmutableMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

public class InMemoryMongoDbLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMongoDb.class); 
	private static final String LOCALHOST = "127.0.0.1";
	private static final int PORT = 27017;
	
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
		EmbeddedMongoInstancesFactory.getServerInstance().addEmbeddedInstance(embeddedMongo(), targetPath);
		LOGGER.info("Started EmbeddedInMemory MongoDb instance.");

	}

	private MongodExecutable embeddedMongo(){

		ImmutableMongodConfig mongodConfig = MongodConfig.builder()
				.version(Version.V4_4_1)
//				.net(new Net(host, port, Network.localhostIsIPv6()))
				.build();

		MongodStarter mongodStarter = MongodStarter.getDefaultInstance();
		return mongodStarter.prepare(mongodConfig);
	}

	private MongoClient fongo(String targetPath) {
		MongoClientSettings clientSettings = MongoClientSettings.builder()
				.applyConnectionString(new ConnectionString(targetPath))
				.build();

		return MongoClients.create(clientSettings);
	}
	
	@Override
	public void doStop() {
		
		LOGGER.info("Stopping EmbeddedInMemory MongoDb instance.");
		
		EmbeddedMongoInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
		MongodExecutable embeddedByTargetPath = EmbeddedMongoInstancesFactory.getServerInstance().getEmbeddedByTargetPath(targetPath);
		embeddedByTargetPath.stop();
		EmbeddedMongoInstancesFactory.getServerInstance().removeEmbeddedInstance(targetPath);

		LOGGER.info("Stopped EmbeddedInMemory MongoDb instance.");
		
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public String getTargetPath() {
		return targetPath;
	}
	
}
