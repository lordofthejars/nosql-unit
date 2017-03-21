package com.lordofthejars.nosqlunit.mongodb.replicaset;

import org.bson.Document;

public class Settings {

	private Document settings;
	
	protected Settings(Document settings) {
		this.settings = settings;
	}
	
	public Document getSettings() {
		return settings;
	}
	
}
