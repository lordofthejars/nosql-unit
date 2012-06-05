package com.lordofthejars.nosqlunit.core;


public interface DatabaseOperation {

	void insert(String dataScript);
	void deleteAll();
	boolean databaseIs(String expectedData);
	void insertNotPresent(String dataScript);
}
