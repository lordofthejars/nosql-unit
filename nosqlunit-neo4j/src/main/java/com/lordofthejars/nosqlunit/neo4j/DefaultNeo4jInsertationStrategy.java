package com.lordofthejars.nosqlunit.neo4j;

import java.io.InputStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.lordofthejars.nosqlunit.graph.parser.GraphMLReader;

public class DefaultNeo4jInsertationStrategy implements Neo4jInsertationStrategy {

	@Override
	public void insert(Neo4jConnectionCallback connection, InputStream dataset) throws Throwable {
		
		GraphDatabaseService graphDatabaseService = connection.graphDatabaseService();
		Transaction tx = graphDatabaseService.beginTx();
		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		try {
			graphMLReader.read(dataset);
			tx.success();
		} finally {
			tx.finish();
		}

	}

}
