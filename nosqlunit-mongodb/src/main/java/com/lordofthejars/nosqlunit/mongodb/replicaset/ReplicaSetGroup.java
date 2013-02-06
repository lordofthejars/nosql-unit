package com.lordofthejars.nosqlunit.mongodb.replicaset;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static org.hamcrest.CoreMatchers.is;

import java.util.LinkedList;
import java.util.List;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;

public class ReplicaSetGroup {

	private static final int DEFAULT_DEFAULT_CONNECTION_INDEX = 0; 
	
	
	private List<ManagedMongoDbLifecycleManager> servers = new LinkedList<ManagedMongoDbLifecycleManager>();
	private ConfigurationDocument configurationDocument;
	
	private String username;
	private String password;
	
	private int connectionIndex = DEFAULT_DEFAULT_CONNECTION_INDEX;
	
	public void setConfigurationDocument(ConfigurationDocument configurationDocument) {
		this.configurationDocument = configurationDocument;
	}
	
	public void addServer(ManagedMongoDbLifecycleManager server) {
		this.servers.add(server);
	}
	
	public ConfigurationDocument getConfiguration() {
		return configurationDocument;
	}
	
	public List<ManagedMongoDbLifecycleManager> getServers() {
		return servers;
	}
	
	public ManagedMongoDbLifecycleManager getDefaultConnection() {
		return servers.get(connectionIndex);
	}
	
	public void setConnectionIndex(int connectionIndex) {
		this.connectionIndex = connectionIndex;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getUsername() {
		return username;
	}
	
	public String getPassword() {
		return password;
	}
	
	public boolean isAuthenticationSet() {
		return this.username != null && this.password != null;
	}
	
	public ManagedMongoDbLifecycleManager getStoppedServer(int port) {
		return selectFirst(
				this.servers,
				having(on(ManagedMongoDbLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedMongoDbLifecycleManager.class).isReady(), is(false))));
	}
	
	public ManagedMongoDbLifecycleManager getStartedServer(int port) {
		return selectFirst(
				this.servers,
				having(on(ManagedMongoDbLifecycleManager.class).getPort(),
						is(port)).and(having(on(ManagedMongoDbLifecycleManager.class).isReady(), is(true))));
	}
	
	public int numberOfStartedServers() {
		return this.servers.size() - numberOfStoppedServers();
	}
	
	public int numberOfStoppedServers() {
		int numberOfStoppedServers = 0;
		
		for (ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager : this.servers) {
			if(!managedMongoDbLifecycleManager.isReady()) {
				numberOfStoppedServers++;
			}
		}
		
		return numberOfStoppedServers;
	}

	public String getReplicaSetName() {
		return this.configurationDocument.getReplicaSetName();
	}

}
