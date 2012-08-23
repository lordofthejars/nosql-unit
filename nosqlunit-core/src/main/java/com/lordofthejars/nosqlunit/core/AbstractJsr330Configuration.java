package com.lordofthejars.nosqlunit.core;

public abstract class AbstractJsr330Configuration implements Configuration {

	protected String connectionIdentifier = "";

	public String getConnectionIdentifier() {
		return connectionIdentifier;
	}
	
	public void setConnectionIdentifier(String connectionIdentifier) {
		this.connectionIdentifier = connectionIdentifier;
	}
	
}
