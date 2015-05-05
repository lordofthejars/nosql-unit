package com.lordofthejars.nosqlunit.neo4j;

import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jLowLevelOps {

	private static final String GET_ALL_NODES_QUERY = "MATCH (nodes) return nodes";
	
	private static final String GET_ALL_RELATIONSHIPS_QUERY = "match ()-[relationships]-() return relationships";
	
	private Neo4jLowLevelOps() {
		super();
	}
	
	public static Iterator<Relationship> getAllRelationships(GraphDatabaseService graphDatabaseService) {
		
		Iterator<Relationship> allRelationships = null;
		
		if(isRemoteConnection(graphDatabaseService)) {
			allRelationships = getAllRelationshipsFromRemote(graphDatabaseService);			
		} else {
			allRelationships = getAllRelationshipsFromEmbedded(graphDatabaseService);
		}
		
		return allRelationships;
		
	}
	
	public static Iterator<Node> getAllNodes(GraphDatabaseService graphDatabaseService) {
		
		Iterator<Node> allNodes = null;
		
		if(isRemoteConnection(graphDatabaseService)) {
			allNodes = getAllNodesFromRemote(graphDatabaseService);			
		} else {
			allNodes = getAllNodesFromEmbedded(graphDatabaseService);
		}
		return allNodes;
		
	}

	private static boolean isRemoteConnection(GraphDatabaseService graphDatabaseService) {
		return graphDatabaseService instanceof RestGraphDatabase;
	}

	private static Iterator<Node> getAllNodesFromEmbedded(GraphDatabaseService graphDatabaseService) {
		GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(graphDatabaseService);
		return globalGraphOperations.getAllNodes().iterator();
	}
	
	private static Iterator<Relationship> getAllRelationshipsFromEmbedded(GraphDatabaseService graphDatabaseService) {
		GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(graphDatabaseService);
		return globalGraphOperations.getAllRelationships().iterator();
	}

	private static Iterator<Node> getAllNodesFromRemote(GraphDatabaseService graphDatabaseService) {
		RestAPI restAPI = ((RestGraphDatabase)graphDatabaseService).getRestAPI();
	    RestCypherQueryEngine restCypherQueryEngine = new RestCypherQueryEngine(restAPI);
	    
	    Iterator<Node> iterator = restCypherQueryEngine.query(GET_ALL_NODES_QUERY, new HashMap<String, Object>()).to(Node.class).iterator();
		return iterator;
	}
	
	private static Iterator<Relationship> getAllRelationshipsFromRemote(GraphDatabaseService graphDatabaseService) {
		RestAPI restAPI = ((RestGraphDatabase)graphDatabaseService).getRestAPI();
	    RestCypherQueryEngine restCypherQueryEngine = new RestCypherQueryEngine(restAPI);
	    
	    Iterator<Relationship> iterator = restCypherQueryEngine.query(GET_ALL_RELATIONSHIPS_QUERY, new HashMap<String, Object>()).to(Relationship.class).iterator();
		return iterator;
	}
	
}
