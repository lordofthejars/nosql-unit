package com.lordofthejars.nosqlunit.redis;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.redis.embedded.EmbeddedRedisBuilder;

public class EmbeddedRedisLifecycleManager extends AbstractLifecycleManager {

private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedRedis.class); 
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = ManagedRedisLifecycleManager.DEFAULT_PORT;

	public static final String INMEMORY_REDIS_TARGET_PATH = "target" + File.separatorChar + "redis-test-data"
			+ File.separatorChar + "impermanent-db";
	
	private String targetPath = INMEMORY_REDIS_TARGET_PATH;
	
	private Jedis jedis;
	
	public EmbeddedRedisLifecycleManager() {
		super();
	}
	
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
		LOGGER.info("Starting Embedded InMemory Redis instance.");
		jedis = createEmbeddedRedis();
		EmbeddedRedisInstances.getInstance().addJedis(jedis, targetPath);
		LOGGER.info("Started Embedded InMemory Redis instance.");
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping Embedded InMemory Redis instance.");
		EmbeddedRedisInstances.getInstance().removeJedis(targetPath);
		LOGGER.info("Stopped Embedded InMemory Redis instance.");
	}

	private Jedis createEmbeddedRedis() {
		EmbeddedRedisBuilder embeddedRedisBuilder = new EmbeddedRedisBuilder();
		return  embeddedRedisBuilder.createEmbeddedJedis();	
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public String getTargetPath() {
		return targetPath;
	}
}
