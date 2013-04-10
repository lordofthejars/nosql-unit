package com.lordofthejars.nosqlunit.redis.replication;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static org.hamcrest.CoreMatchers.is;

import java.util.LinkedList;
import java.util.List;

import com.lordofthejars.nosqlunit.redis.ManagedRedisLifecycleManager;

public class ReplicationGroup {

	private static final int MASTER_SERVER = 1;
	private final ManagedRedisLifecycleManager master;
	private List<ManagedRedisLifecycleManager> slaveServers = new LinkedList<ManagedRedisLifecycleManager>();
	
	public ReplicationGroup(ManagedRedisLifecycleManager managedRedisLifecycleManager) {
		this.master = managedRedisLifecycleManager;
	}
	
	public void addSlaveServer(ManagedRedisLifecycleManager managedRedisLifecycleManager) {
		this.slaveServers.add(managedRedisLifecycleManager);
	}
	
	protected List<ManagedRedisLifecycleManager> getSlaveServers() {
		return slaveServers;
	}
	
	protected ManagedRedisLifecycleManager getMaster() {
		return master;
	}

	public ManagedRedisLifecycleManager getStoppedServer(int port) {
		
		if(this.master.getPort() == port && !this.master.isReady()) {
			return this.master;
		}
		
		return selectFirst(
				this.slaveServers,
				having(on(ManagedRedisLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedRedisLifecycleManager.class).isReady(), is(false))));
	}
	
	public ManagedRedisLifecycleManager getStartedServer(int port) {
		
		if(this.master.getPort() == port && this.master.isReady()) {
			return this.master;
		}
		
		
		return selectFirst(
				this.slaveServers,
				having(on(ManagedRedisLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedRedisLifecycleManager.class).isReady(), is(true))));
	}
	
	public int numberOfStartedServers() {
		return this.slaveServers.size() + MASTER_SERVER - numberOfStoppedServers();
	}
	
	public int numberOfStoppedServers() {
		int numberOfStoppedServers = 0;
		
		if(!this.master.isReady()) {
			numberOfStoppedServers++;
		}
		
		for (ManagedRedisLifecycleManager managedRedisLifecycleManager : this.slaveServers) {
			if(!managedRedisLifecycleManager.isReady()) {
				numberOfStoppedServers++;
			}
		}
		
		return numberOfStoppedServers;
	}
	
}
