package com.lordofthejars.nosqlunit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Neo4jConnectionCallback {

	GraphDatabaseService graphDatabaseService();
	
}
