package com.lordofthejars.nosqlunit.redis;

import org.junit.rules.ExternalResource;

public class ManagedRedis extends ExternalResource {

	protected ManagedRedisLifecycleManager managedRedisLifecycleManager;

	private ManagedRedis() {
		super();
	}

	public static class ManagedRedisRuleBuilder {

		private ManagedRedisLifecycleManager managedRedisLifecycleManager;

		private ManagedRedisRuleBuilder() {
			this.managedRedisLifecycleManager = new ManagedRedisLifecycleManager();
		}

		public static ManagedRedisRuleBuilder newManagedRedisRule() {
			return new ManagedRedisRuleBuilder();
		}

		public ManagedRedisRuleBuilder port(int port) {
			this.managedRedisLifecycleManager.setPort(port);
			return this;
		}

		public ManagedRedisRuleBuilder targetPath(String targetPath) {
			this.managedRedisLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public ManagedRedisRuleBuilder redisPath(String redisPath) {
			this.managedRedisLifecycleManager.setRedisPath(redisPath);
			return this;
		}

		public ManagedRedisRuleBuilder slaveOf(String masterHost, int masterPort) {
			this.managedRedisLifecycleManager.setMasterHost(masterHost);
			this.managedRedisLifecycleManager.setMasterPort(masterPort);
			
			return this;
		}
		
		public ManagedRedisRuleBuilder configurationPath(String configurationPath) {
			this.managedRedisLifecycleManager.setConfigurationFilepath(configurationPath);
			return this;
		}

		public ManagedRedisRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedRedisLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public ManagedRedisRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedRedisLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}
		
		public ManagedRedis build() {
			
			if (this.managedRedisLifecycleManager.getRedisPath() == null) {
				throw new IllegalArgumentException("Redis Path cannot be null.");
			}

			ManagedRedis managedRedis = new ManagedRedis();
			managedRedis.managedRedisLifecycleManager = this.managedRedisLifecycleManager;
			
			return managedRedis;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.managedRedisLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.managedRedisLifecycleManager.stopEngine();
	}
	
	

}
