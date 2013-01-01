package com.lordofthejars.nosqlunit.redis;

import org.junit.rules.ExternalResource;

public class EmbeddedRedis extends ExternalResource {

	private EmbeddedRedis() {
		super();
	}
	
	protected EmbeddedRedisLifecycleManager embeddedRedisLifecycleManager;
	
	public static class EmbeddedRedisRuleBuilder {

		private EmbeddedRedisLifecycleManager embeddedRedisLifecycleManager;

		private EmbeddedRedisRuleBuilder() {
			this.embeddedRedisLifecycleManager= new EmbeddedRedisLifecycleManager();
		}

		public static EmbeddedRedisRuleBuilder newEmbeddedRedisRule() {
			return new EmbeddedRedisRuleBuilder();
		}

		public EmbeddedRedisRuleBuilder targetPath(String targetPath) {
			this.embeddedRedisLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedRedis build() {
			
			if (this.embeddedRedisLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Redis is provided.");
			}
			
			EmbeddedRedis embeddedRedis = new EmbeddedRedis();
			embeddedRedis.embeddedRedisLifecycleManager = this.embeddedRedisLifecycleManager;
			
			return embeddedRedis;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.embeddedRedisLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.embeddedRedisLifecycleManager.stopEngine();
	}
	
	
}
