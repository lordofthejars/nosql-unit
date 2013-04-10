package com.lordofthejars.nosqlunit.redis.replication;

import com.lordofthejars.nosqlunit.redis.ManagedRedisLifecycleManager;

public class ReplicationGroupBuilder {

	private ReplicationGroup replicationGroup;
	
	private ReplicationGroupBuilder() {
		super();
	}
	
	public static ReplicationGroupBuilder master(ManagedRedisLifecycleManager master) {
		ReplicationGroupBuilder replicationGroupBuilder = new ReplicationGroupBuilder();
		replicationGroupBuilder.replicationGroup = new ReplicationGroup(master);
		
		return replicationGroupBuilder;
	}
	
	public ReplicationGroupBuilder slave(ManagedRedisLifecycleManager slave) {
		this.replicationGroup.addSlaveServer(slave);
		
		return this;
	}
	
	public ReplicationManagedRedis get() {
		return new ReplicationManagedRedis(this.replicationGroup);
	}
	
}
