package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;


public interface DatabaseOperation<T> {

	void insert(InputStream dataScript);
	void deleteAll();
	boolean databaseIs(InputStream expectedData);
	void insertNotPresent(InputStream dataScript);
	
	T connectionManager();
}
