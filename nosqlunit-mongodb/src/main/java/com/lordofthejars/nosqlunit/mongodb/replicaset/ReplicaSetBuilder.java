package com.lordofthejars.nosqlunit.mongodb.replicaset;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class ReplicaSetBuilder {

	private static final int DEFAULT_VERSION = 1;
	private static final String ID_TAG = "_id";
	private static final String PRIORITY_TAG = "priority";
	private static final String HIDDEN_TAG = "hidden";
	private static final String HOST_TAG = "host";
	private static final String VERSION_TAG = "version";
	private static final String SLAVE_DELAY_TAG = "slaveDelay";
	private static final String ARBITER_TAG = "arbiterOnly";
	private static final String VOTES_TAG = "votes";
	private static final String TAGS_TAG = "tags";
	private static final String BUILD_INDEXES_TAG = "buildIndexes";
	private static final String MEMBERS_TAG = "members";
	private static final String SETTINGS_TAG = "settings";
	
	private String replicaSetName;

	private int numberOfServers = 0;

	private ReplicaSetGroup replicaSetGroup = new ReplicaSetGroup();
	private List<DBObject> configuration = new LinkedList<DBObject>();
	
	private Settings settings;
	
	private ReplicaSetBuilder() {
		super();
	}

	public static ReplicaSetBuilder replicaSet(String name) {
		ReplicaSetBuilder replicaSetBuilder = new ReplicaSetBuilder();
		replicaSetBuilder.replicaSetName = name;
		return replicaSetBuilder;
	}

	public static ReplicaSetBuilder replicaSet(String name, Settings settings) {
		ReplicaSetBuilder replicaSetBuilder = new ReplicaSetBuilder();
		replicaSetBuilder.replicaSetName = name;
		replicaSetBuilder.settings = settings;
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
		final BasicDBObject config = new BasicDBObject(ID_TAG, replicaSetName);
		config.put(VERSION_TAG, DEFAULT_VERSION);
		config.put(MEMBERS_TAG, this.configuration);
		
		if(settings != null) {
			config.put(SETTINGS_TAG, settings.getSettings());
		}
		
		replicaSetGroup.setConfigurationDocument(new ConfigurationDocument(config));
		
		return new ReplicaSetManagedMongoDb(replicaSetGroup);
	}
	
	public void addConfiguration(DBObject dbObject) {
		this.configuration.add(dbObject);
	}
	
	private String host(ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager) {
		return managedMongoDbLifecycleManager.getHost() + ":" + managedMongoDbLifecycleManager.getPort();
	}
	
	public class CustomConfigurationBuilder {
		
		private DBObject dbObject = new BasicDBObject();
		private ReplicaSetBuilder parent;
		
		private CustomConfigurationBuilder(ReplicaSetBuilder replicaSetBuilder, String host) {
			parent = replicaSetBuilder;
			
			dbObject.put(ID_TAG, parent.numberOfServers);
			dbObject.put(HOST_TAG, host);
			
			parent.numberOfServers++;
		}
		
		public CustomConfigurationBuilder priority(int priority) {
			dbObject.put(PRIORITY_TAG, priority);
			return this;
		}
		
		public CustomConfigurationBuilder slaveDelay(long time, TimeUnit unit) {
			dbObject.put(SLAVE_DELAY_TAG, TimeUnit.SECONDS.convert(time, unit));
			return this;
		}
		
		public CustomConfigurationBuilder arbiterOnly() {
			dbObject.put(ARBITER_TAG, true);
			return this;
		}
		
		public CustomConfigurationBuilder votes(int votes) {
			dbObject.put(VOTES_TAG, votes);
			return this;
		}
		
		public CustomConfigurationBuilder buildIndexes() {
			dbObject.put(BUILD_INDEXES_TAG, true);
			return this;
		}
		
		public CustomConfigurationBuilder tags(String ... tags) {
			
			BasicDBObjectBuilder basicDBObjectBuilder = new BasicDBObjectBuilder();
			
			for(int i=0;i<tags.length;i+=2) {
				basicDBObjectBuilder.append(tags[i], tags[i+1]);
			}
			
			dbObject.put(TAGS_TAG, basicDBObjectBuilder.get());
			
			return this;
		}
		
		public CustomConfigurationBuilder hidden() {
			dbObject.put(HIDDEN_TAG, true);
			return this;
		}
		
		public ReplicaSetBuilder configure() {
			this.parent.addConfiguration(dbObject);
			return this.parent;
		}
		
	}

	public static class SettingsBuilder {
		
		private static final String GET_LAST_ERROR_DEFAULTS_TAG = "getLastErrorDefaults";
		private static final String GET_LAST_ERROR_MODES_TAG = "getLastErrorModes";
		
		private BasicDBObjectBuilder basicDBObjectBuilder = new BasicDBObjectBuilder();
		
		private SettingsBuilder() {
			super();
		}
		
		public static SettingsBuilder settings() {
			return new SettingsBuilder();
		}
		
		public SettingsBuilder getLastErrorModes(String jsonDocument) {
			DBObject modes = (DBObject) JSON.parse(jsonDocument);
			basicDBObjectBuilder.append(GET_LAST_ERROR_MODES_TAG, modes);
			return this;
		}
		
		public SettingsBuilder getLastErrorDefaults(String jsonDocument) {
			DBObject error = (DBObject) JSON.parse(jsonDocument);
			basicDBObjectBuilder.append(GET_LAST_ERROR_DEFAULTS_TAG, error);
			return this;
		}
		
		public Settings get() {
			return new Settings(basicDBObjectBuilder.get());
		}
		
	}
	
}
