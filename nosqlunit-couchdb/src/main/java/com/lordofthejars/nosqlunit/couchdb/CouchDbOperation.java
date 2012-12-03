package com.lordofthejars.nosqlunit.couchdb;

import java.io.InputStream;

import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.RestTemplate;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CouchDbOperation implements DatabaseOperation<CouchDbConnector> {

	private CouchDbConnector couchDbConnector;
	private DataLoader dataLoader;
	
	public CouchDbOperation(CouchDbConnector couchDbConnector) {
		this.couchDbConnector = couchDbConnector;
		this.dataLoader = new DataLoader(this.couchDbConnector);
	}
	
	@Override
	public void insert(InputStream dataScript) {
		insertDocuments(dataScript);
	}

	private void insertDocuments(InputStream dataScript) {
		this.dataLoader.load(dataScript);
	}

	@Override
	public void deleteAll() {
		removeDatabase();
	}

	private void removeDatabase() {
		HttpClient connection = couchDbConnector.getConnection();
		RestTemplate restTemplate = new RestTemplate(connection);
		restTemplate.delete(couchDbConnector.path());
		
		couchDbConnector.createDatabaseIfNotExists();
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		CouchDbAssertion.strictAssertEquals(expectedData, couchDbConnector);
		return true;
	}

	@Override
	public CouchDbConnector connectionManager() {
		return this.couchDbConnector;
	}
	
}
