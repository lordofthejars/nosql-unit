package com.lordofthejars.nosqlunit.couchdb;

import java.io.InputStream;

public class DefaultCouchDbInsertionStrategy implements CouchDbInsertionStrategy {

	@Override
	public void insert(CouchDbConnectionCallback connection, InputStream dataset) throws Throwable {
		DataLoader dataLoader = new DataLoader(connection.couchDbConnector());
		insertDocuments(dataLoader, dataset);
	}

	private void insertDocuments(DataLoader dataLoader, InputStream dataScript) {
		dataLoader.load(dataScript);
	}
	
}
