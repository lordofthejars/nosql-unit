package com.lordofthejars.nosqlunit.redis;

import redis.clients.jedis.BinaryJedisCommands;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public abstract class AbstractRedisConfiguration extends AbstractJsr330Configuration {

	protected DatabaseOperation<? extends BinaryJedisCommands> databaseOperation;
	
	public void setDatabaseOperation(DatabaseOperation<? extends BinaryJedisCommands> databaseOperation) {
		this.databaseOperation = databaseOperation;
	}
	
	public DatabaseOperation<? extends BinaryJedisCommands> getDatabaseOperation() {
		return this.databaseOperation;
	}
	
}
