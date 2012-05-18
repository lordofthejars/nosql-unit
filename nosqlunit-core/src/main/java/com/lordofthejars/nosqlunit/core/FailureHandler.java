package com.lordofthejars.nosqlunit.core;

public class FailureHandler {

	
	public static final  Error createFailure(String message, Object...args) {
		return new NoSqlAssertionError(String.format(message, args));
	}
	
}
