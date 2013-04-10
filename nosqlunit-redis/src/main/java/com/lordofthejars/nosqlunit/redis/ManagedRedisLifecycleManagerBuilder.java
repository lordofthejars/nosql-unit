package com.lordofthejars.nosqlunit.redis;


public class ManagedRedisLifecycleManagerBuilder {

	private ManagedRedisLifecycleManager managedRedisLifecycleManager;

	private ManagedRedisLifecycleManagerBuilder() {
		this.managedRedisLifecycleManager = new ManagedRedisLifecycleManager();
	}

	public static ManagedRedisLifecycleManagerBuilder newManagedRedis() {
		return new ManagedRedisLifecycleManagerBuilder();
	}

	public ManagedRedisLifecycleManagerBuilder port(int port) {
		this.managedRedisLifecycleManager.setPort(port);
		return this;
	}

	public ManagedRedisLifecycleManagerBuilder targetPath(String targetPath) {
		this.managedRedisLifecycleManager.setTargetPath(targetPath);
		return this;
	}

	public ManagedRedisLifecycleManagerBuilder redisPath(String redisPath) {
		this.managedRedisLifecycleManager.setRedisPath(redisPath);
		return this;
	}

	public ManagedRedisLifecycleManagerBuilder slaveOf(String masterHost, int masterPort) {
		this.managedRedisLifecycleManager.setMasterHost(masterHost);
		this.managedRedisLifecycleManager.setMasterPort(masterPort);
		
		return this;
	}
	
	public ManagedRedisLifecycleManagerBuilder configurationPath(String configurationPath) {
		this.managedRedisLifecycleManager.setConfigurationFilepath(configurationPath);
		return this;
	}

	public ManagedRedisLifecycleManagerBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
		this.managedRedisLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
		return this;
	}

	public ManagedRedisLifecycleManagerBuilder appendSingleCommandLineArguments(String argument) {
		this.managedRedisLifecycleManager.addSingleCommandLineArgument(argument);
		return this;
	}
	
	public ManagedRedisLifecycleManager build() {
		
		if (this.managedRedisLifecycleManager.getRedisPath() == null) {
			throw new IllegalArgumentException("Redis Path cannot be null.");
		}

		return managedRedisLifecycleManager;
	}
}
