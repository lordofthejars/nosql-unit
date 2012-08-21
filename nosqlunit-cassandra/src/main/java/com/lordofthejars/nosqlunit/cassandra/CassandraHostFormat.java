package com.lordofthejars.nosqlunit.cassandra;

public class CassandraHostFormat {

	private static final String SEPARATOR = ":";
	
	private CassandraHostFormat() {
		super();
	}
	
	public static final String convert(String host, int port) {
		return host+SEPARATOR+port;
	}
	
}
