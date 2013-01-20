package com.lordofthejars.nosqlunit.mongodb.replicaset;

import com.mongodb.DBObject;

public class Settings {

	private DBObject settings;
	
	protected Settings(DBObject settings) {
		this.settings = settings;
	}
	
	public DBObject getSettings() {
		return settings;
	}
	
}
