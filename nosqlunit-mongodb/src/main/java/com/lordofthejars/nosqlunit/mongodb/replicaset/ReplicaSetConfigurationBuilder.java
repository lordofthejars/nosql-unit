package com.lordofthejars.nosqlunit.mongodb.replicaset;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicaSetConfigurationBuilder {

	public static final String ID_TAG = "_id";
	public static final String PRIORITY_TAG = "priority";
	public static final String HIDDEN_TAG = "hidden";
	public static final String HOST_TAG = "host";
	public static final String VERSION_TAG = "version";
	public static final String SLAVE_DELAY_TAG = "slaveDelay";
	public static final String ARBITER_TAG = "arbiterOnly";
	public static final String VOTES_TAG = "votes";
	public static final String TAGS_TAG = "tags";
	public static final String BUILD_INDEXES_TAG = "buildIndexes";
	public static final String MEMBERS_TAG = "members";
	public static final String SETTINGS_TAG = "settings";
	
	private Document configurationBuilder;
	private List<Document> members;
	
	//We do not insert directly to add settings at the bottom of the document. Only aesthetic matter. 
	private Settings settings;
	
	private int numberOfMembers = 0;
	
	private ReplicaSetConfigurationBuilder() {
		super();
		this.configurationBuilder = new Document();
		this.members = new ArrayList<>();
	}
	
	public static final ReplicaSetConfigurationBuilder replicaSetConfiguration(String replicaSetName) {
		ReplicaSetConfigurationBuilder replicaSetConfigurationBuilder = new ReplicaSetConfigurationBuilder();
		replicaSetConfigurationBuilder.replicaSetName(replicaSetName);
		return replicaSetConfigurationBuilder;
	}
	
	public ReplicaSetConfigurationBuilder replicaSetName(String replicaSetName) {
		this.configurationBuilder.append(ID_TAG, replicaSetName);
		return this;
	}
	
	public ReplicaSetConfigurationBuilder version(int version) {
		this.configurationBuilder.append(VERSION_TAG, version);
		return this;
	}
	
	public ReplicaSetConfigurationBuilder settings(Settings settings) {
		this.settings = settings;
		return this;
	}
	
	public MemberConfigurationBuilder member(String host) {
		return new MemberConfigurationBuilder(this, host);
	}
	
	public ConfigurationDocument get() {
		this.configurationBuilder.append(MEMBERS_TAG, this.members);
		
		if(this.settings != null) {
			this.configurationBuilder.append(SETTINGS_TAG, this.settings.getSettings());
		}
		
		return new ConfigurationDocument(this.configurationBuilder);
	}
	
	private void addMember(Document member) {
		this.members.add(member);
	}
	
	public class MemberConfigurationBuilder {
		
		private Document dbObject = new Document();
		private ReplicaSetConfigurationBuilder parent;
		
		private MemberConfigurationBuilder(ReplicaSetConfigurationBuilder replicaSetBuilder, String host) {
			parent = replicaSetBuilder;
			
			dbObject.put(ID_TAG, parent.numberOfMembers);
			dbObject.put(HOST_TAG, host);
			
			parent.numberOfMembers++;
		}
		
		public MemberConfigurationBuilder priority(int priority) {
			dbObject.put(PRIORITY_TAG, priority);
			return this;
		}
		
		public MemberConfigurationBuilder slaveDelay(long time, TimeUnit unit) {
			dbObject.put(SLAVE_DELAY_TAG, TimeUnit.SECONDS.convert(time, unit));
			return this;
		}
		
		public MemberConfigurationBuilder arbiterOnly() {
			dbObject.put(ARBITER_TAG, true);
			return this;
		}
		
		public MemberConfigurationBuilder votes(int votes) {
			dbObject.put(VOTES_TAG, votes);
			return this;
		}
		
		public MemberConfigurationBuilder buildIndexes() {
			dbObject.put(BUILD_INDEXES_TAG, true);
			return this;
		}
		
		public MemberConfigurationBuilder tags(String ... tags) {
			
			Document basicDBObjectBuilder = new Document();
			
			for(int i=0;i<tags.length;i+=2) {
				basicDBObjectBuilder.append(tags[i], tags[i+1]);
			}
			
			dbObject.put(TAGS_TAG, basicDBObjectBuilder);
			
			return this;
		}
		
		public MemberConfigurationBuilder hidden() {
			dbObject.put(HIDDEN_TAG, true);
			return this;
		}
		
		public ReplicaSetConfigurationBuilder configure() {
			this.parent.addMember(dbObject);
			return this.parent;
		}
		
	}
	
	public static class SettingsBuilder {
		
		private static final String GET_LAST_ERROR_DEFAULTS_TAG = "getLastErrorDefaults";
		private static final String GET_LAST_ERROR_MODES_TAG = "getLastErrorModes";
		
		private Document basicDBObjectBuilder = new Document();
		
		private SettingsBuilder() {
			super();
		}
		
		public static SettingsBuilder settings() {
			return new SettingsBuilder();
		}
		
		public SettingsBuilder getLastErrorModes(String jsonDocument) {
			Document modes = Document.parse(jsonDocument);
			basicDBObjectBuilder.append(GET_LAST_ERROR_MODES_TAG, modes);
			return this;
		}
		
		public SettingsBuilder getLastErrorDefaults(String jsonDocument) {
			Document error = Document.parse(jsonDocument);
			basicDBObjectBuilder.append(GET_LAST_ERROR_DEFAULTS_TAG, error);
			return this;
		}
		
		public Settings get() {
			return new Settings(basicDBObjectBuilder);
		}
		
	}
	
}
