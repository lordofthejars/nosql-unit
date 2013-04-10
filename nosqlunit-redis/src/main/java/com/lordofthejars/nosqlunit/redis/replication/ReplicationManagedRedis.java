package com.lordofthejars.nosqlunit.redis.replication;

import java.util.List;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.redis.ManagedRedisLifecycleManager;

public class ReplicationManagedRedis extends ExternalResource {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ReplicationManagedRedis.class);
	
	private ReplicationGroup replicationGroup;

	protected ReplicationManagedRedis(ReplicationGroup replicationGroup) {
		this.replicationGroup = replicationGroup;
	}

	public void startupServer(int port) throws Throwable {

		ManagedRedisLifecycleManager stoppedServer = replicationGroup.getStoppedServer(port);

		if (stoppedServer != null) {
			stoppedServer.startEngine();
		}

	}
	
	public void stopServer(int port) throws Throwable {

		ManagedRedisLifecycleManager stoppedServer = replicationGroup.getStartedServer(port);

		if (stoppedServer != null) {
			stoppedServer.stopEngine();
		}

	}
	
	@Override
	protected void before() throws Throwable {
		wakeUpServers();
	}

	private void wakeUpServers() throws Throwable {
		
		LOGGER.info("Starting Redis Master Server");
		
		startMaster();
		
		LOGGER.info("Started Redis Master Server");
		
		LOGGER.info("Starting Redis Slave Servers");
		
		startSlaves();
		
		LOGGER.info("Started Redis Slave Servers");
		
	}

	private void startSlaves() throws Throwable {
		List<ManagedRedisLifecycleManager> slaveServers = replicationGroup.getSlaveServers();
		
		for (ManagedRedisLifecycleManager slaveServer : slaveServers) {
			if(isServerStopped(slaveServer)) {
				slaveServer.startEngine();
			}
		}
		
	}
	
	private void startMaster() throws Throwable {
		ManagedRedisLifecycleManager master = replicationGroup.getMaster();
		
		if(isServerStopped(master)) {
			master.startEngine();
		}
	}
	
	private boolean isServerStopped(ManagedRedisLifecycleManager master) {
		return !master.isReady();
	}

	@Override
	protected void after() {
		shutdownServers();
	}

	private void shutdownServers() {

		LOGGER.info("Stopping Redis Master Server");
		
		stopMaster();
		
		LOGGER.info("Stopped Redis Master Server");
		
		LOGGER.info("Stopping Redis Slave Servers");
		
		stopSlaves();
		
		LOGGER.info("Stopped Redis Slave Servers");
	}

	private void stopMaster() {
		
		ManagedRedisLifecycleManager master = replicationGroup.getMaster();
		
		if(isServerStarted(master)) {
			master.stopEngine();
		}
		
	}
	
	private void stopSlaves() {
		List<ManagedRedisLifecycleManager> slaveServers = replicationGroup.getSlaveServers();
		
		for (ManagedRedisLifecycleManager slaveServer : slaveServers) {
			
			if(isServerStarted(slaveServer)) {
				slaveServer.stopEngine();
			}
			
		}
		
	}
	
	private boolean isServerStarted(ManagedRedisLifecycleManager master) {
		return master.isReady();
	}
	
}
