package com.lordofthejars.nosqlunit.mongodb.replicaset;

import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetConfigurationBuilder.replicaSetConfiguration;

import java.util.concurrent.TimeUnit;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetConfigurationBuilder.MemberConfigurationBuilder;

public class ReplicaSetBuilder {

	private static final int DEFAULT_VERSION = 1;
	
	private String replicaSetName;

	private ReplicaSetGroup replicaSetGroup = new ReplicaSetGroup();
	
	private ReplicaSetConfigurationBuilder replicaSetConfigurationBuilder;
	
	private ReplicaSetBuilder() {
		super();
	}

	public static ReplicaSetBuilder replicaSet(String name) {
		ReplicaSetBuilder replicaSetBuilder = new ReplicaSetBuilder();
		replicaSetBuilder.replicaSetName = name;
		replicaSetBuilder.replicaSetConfigurationBuilder = replicaSetConfiguration(name).version(DEFAULT_VERSION);
		return replicaSetBuilder;
	}

	public static ReplicaSetBuilder replicaSet(String name, Settings settings) {
		ReplicaSetBuilder replicaSetBuilder = new ReplicaSetBuilder();
		replicaSetBuilder.replicaSetConfigurationBuilder = replicaSetConfiguration(name).version(DEFAULT_VERSION).settings(settings);
		replicaSetBuilder.replicaSetName = name;
		return replicaSetBuilder;
	}

	public ReplicaSetBuilder eligible(ManagedMongoDbLifecycleManager managedInstance) {
		return server(managedInstance).configure();
	}

	public ReplicaSetBuilder secondary(ManagedMongoDbLifecycleManager managedInstance) {
		return server(managedInstance).priority(0).configure();
	}
	
	public ReplicaSetBuilder hidden(ManagedMongoDbLifecycleManager managedInstance) {
		return server(managedInstance).priority(0).hidden().configure();
	}
	
	public ReplicaSetBuilder delayed(ManagedMongoDbLifecycleManager managedInstance, long time, TimeUnit unit) {
		return server(managedInstance).priority(0).slaveDelay(time, unit).configure();
	}
	
	public ReplicaSetBuilder arbiter(ManagedMongoDbLifecycleManager managedInstance) {
		return server(managedInstance).arbiterOnly().configure();
	}
	
	public ReplicaSetBuilder noneVoter(ManagedMongoDbLifecycleManager managedInstance) {
		return server(managedInstance).votes(0).configure();
	}
	
	public ReplicaSetBuilder withAuthentication(String username, String password) {
		this.replicaSetGroup.setUsername(username);
		this.replicaSetGroup.setPassword(password);
		return this;
	}
	
	public CustomConfigurationBuilder server(ManagedMongoDbLifecycleManager server) {
		
		if(!server.isReplicaSetNameSet()) {
			server.setReplicaSetName(replicaSetName);
		}
		
		this.replicaSetGroup.addServer(server);
		return new CustomConfigurationBuilder(this, host(server));
	}

	public ReplicaSetManagedMongoDb get() {
		return buildReplicaSetRule();
	}
	
	public ReplicaSetManagedMongoDb get(int index) {
		this.replicaSetGroup.setConnectionIndex(index);
		return buildReplicaSetRule();
	}

	private ReplicaSetManagedMongoDb buildReplicaSetRule() {
		
		replicaSetGroup.setConfigurationDocument(replicaSetConfigurationBuilder.get());
		
		return new ReplicaSetManagedMongoDb(replicaSetGroup);
	}
	
	private String host(ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return managedMongoDbLifecycleManager.getHost() + ":" + managedMongoDbLifecycleManager.getPort();
	}
	
	public class CustomConfigurationBuilder {
		
		private ReplicaSetBuilder parent;
		private MemberConfigurationBuilder memberConfigurationBuilder;
		
		private CustomConfigurationBuilder(ReplicaSetBuilder replicaSetBuilder, String host) {
			parent = replicaSetBuilder;
			memberConfigurationBuilder = parent.replicaSetConfigurationBuilder.member(host);
		}
		
		public CustomConfigurationBuilder priority(int priority) {
			memberConfigurationBuilder.priority(priority);
			return this;
		}
		
		public CustomConfigurationBuilder slaveDelay(long time, TimeUnit unit) {
			memberConfigurationBuilder.slaveDelay(time, unit);
			return this;
		}
		
		public CustomConfigurationBuilder arbiterOnly() {
			memberConfigurationBuilder.arbiterOnly();
			return this;
		}
		
		public CustomConfigurationBuilder votes(int votes) {
			memberConfigurationBuilder.votes(votes);
			return this;
		}
		
		public CustomConfigurationBuilder buildIndexes() {
			memberConfigurationBuilder.buildIndexes();
			return this;
		}
		
		public CustomConfigurationBuilder tags(String ... tags) {
			memberConfigurationBuilder.tags(tags);
			return this;
		}
		
		public CustomConfigurationBuilder hidden() {
			memberConfigurationBuilder.hidden();
			return this;
		}
		
		public ReplicaSetBuilder configure() {
			memberConfigurationBuilder.configure();
			return this.parent;
		}
		
	}
	
}
