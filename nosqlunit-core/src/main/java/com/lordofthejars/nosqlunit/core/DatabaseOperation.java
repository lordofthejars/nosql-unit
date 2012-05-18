package com.lordofthejars.nosqlunit.core;


public interface DatabaseOperation {

	void insert(String dataScript);
	void deleteAll();
	void nonStrictAssertEquals(String expectedData);
	void insertNotPresent(String dataScript);
}
