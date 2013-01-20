package com.lordofthejars.nosqlunit.mongodb.replicaset;

import java.util.LinkedList;
import java.util.List;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;

public class ReplicaSetGroup {

	private static final int DEFAULT_DEFAULT_CONNECTION_INDEX = 0; 
	
	
	private List<ManagedMongoDbLifecycleManager> servers = new LinkedList<ManagedMongoDbLifecycleManager>();
	private ConfigurationDocument configurationDocument;
	
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
}
