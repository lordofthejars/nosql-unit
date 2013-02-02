package com.lordofthejars.nosqlunit.mongodb.shard;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;

public class ShardedGroupBuilder {

	private ShardedGroup shardedGroup;
	
	private ShardedGroupBuilder() {
		super();
		this.shardedGroup = new ShardedGroup();
	}
	
	public static ShardedGroupBuilder shardedGroup() {
		return new ShardedGroupBuilder();
	}
	
	public ShardedGroupBuilder withAuthentication(String username, String password) {
		this.shardedGroup.setUsername(username);
		this.shardedGroup.setPassword(password);
		return this;
	}
	
	public ShardedGroupBuilder shard(ManagedMongoDbLifecycleManager shard) {
		shard.setShardServer(true);
		this.shardedGroup.addShard(shard);
		return this;
	}
	
	public ShardedGroupBuilder config(ManagedMongoDbLifecycleManager config) {
		config.setConfigServer(true);
		this.shardedGroup.addConfig(config);
		return this;
	}
	
	public ShardedGroupBuilder mongos(ManagedMongosLifecycleManager mongos) {
		this.shardedGroup.addMongos(mongos);
		return this;
	}
	
	public ShardedManagedMongoDb get() {
		return new ShardedManagedMongoDb(shardedGroup);
	}
	
}
