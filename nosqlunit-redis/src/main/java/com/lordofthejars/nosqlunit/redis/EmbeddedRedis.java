package com.lordofthejars.nosqlunit.redis;



import java.io.File;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.redis.embedded.EmbeddedRedisBuilder;

public class EmbeddedRedis extends AbstractLifecycleManager {

	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = ManagedRedis.DEFAULT_PORT;

	public static final String INMEMORY_REDIS_TARGET_PATH = "target" + File.separatorChar + "redis-test-data"
			+ File.separatorChar + "impermanent-db";
	
	private String targetPath = INMEMORY_REDIS_TARGET_PATH;
	
	private Jedis jedis;
	
	private EmbeddedRedis() {
		super();
	}
	
	public static class EmbeddedRedisRuleBuilder {

		private EmbeddedRedis embeddedRedis;

		private EmbeddedRedisRuleBuilder() {
			this.embeddedRedis= new EmbeddedRedis();
		}

		public static EmbeddedRedisRuleBuilder newEmbeddedRedisRule() {
			return new EmbeddedRedisRuleBuilder();
		}

		public EmbeddedRedisRuleBuilder targetPath(String targetPath) {
			this.embeddedRedis.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedRedis build() {
			if (this.embeddedRedis.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Redis is provided.");
			}
			return this.embeddedRedis;
		}

	}
	
	@Override
	protected String getHost() {
		return LOCALHOST+targetPath;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	protected void doStart() throws Throwable {
		jedis = createEmbeddedRedis();
		EmbeddedRedisInstances.getInstance().addJedis(jedis, targetPath);
	}

	@Override
	protected void doStop() {
		EmbeddedRedisInstances.getInstance().removeJedis(targetPath);
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
