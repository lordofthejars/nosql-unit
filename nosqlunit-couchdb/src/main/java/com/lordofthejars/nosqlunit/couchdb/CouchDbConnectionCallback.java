package com.lordofthejars.nosqlunit.couchdb;

import org.ektorp.CouchDbConnector;

public interface CouchDbConnectionCallback {

	CouchDbConnector couchDbConnector();
	
}
