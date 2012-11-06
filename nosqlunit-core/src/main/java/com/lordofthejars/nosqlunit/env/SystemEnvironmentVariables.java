package com.lordofthejars.nosqlunit.env;

public class SystemEnvironmentVariables {

	public static final String getPropertyVariable(String property) {
		return System.getProperty(property);
	}
	
	public static final String getEnvironmentVariable(String property) {
		return System.getenv(property);				
	}
	
	public static final String getEnvironmentOrPropertyVariable(String property) {
		
		String environmentVariable = getEnvironmentVariable(property);
		
		if(environmentVariable == null) {
			return getPropertyVariable(property);
		}
		
		return environmentVariable;
		
	}
	
}
