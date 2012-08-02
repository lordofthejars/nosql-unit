package com.lordofthejars.nosqlunit.core;

public class FailureHandler {

	
	public static final  Error createFailure(String message, Object...args) {
		return new NoSqlAssertionError(String.format(message, args));
	}
	
	public static final IllegalStateException createIllegalStateFailure(String message, Object...args) {
		return new IllegalStateException(String.format(message, args));
	}
	
}
