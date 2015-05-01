package com.lordofthejars.nosqlunit.neo4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jOperation extends AbstractCustomizableDatabaseOperation<Neo4jConnectionCallback, GraphDatabaseService> {

	private GraphDatabaseService graphDatabaseService;

	public Neo4jOperation(GraphDatabaseService graphDatabaseService) {
		super();
		this.graphDatabaseService = graphDatabaseService;
		setInsertionStrategy(new DefaultNeo4jInsertionStrategy());
		setComparisonStrategy(new DefaultNeo4jComparisonStrategy());
	}

	@Override
	public void insert(InputStream dataScript) {
		insertData(dataScript);
	}

	private void insertData(InputStream dataScript) {
		try {
			executeInsertion(new Neo4jConnectionCallback() {
				
				@Override
				public GraphDatabaseService graphDatabaseService() {
					return graphDatabaseService;
				}
			}, dataScript);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void deleteAll() {
		Transaction tx = beginTx();

		try {
			
			Iterator<Node> allNodes = Neo4jLowLevelOps.getAllNodes(graphDatabaseService);
			Iterator<Relationship> allRelationships = Neo4jLowLevelOps.getAllRelationships(graphDatabaseService);
			
			removeAllRelationships(allRelationships);
			removeAllNodes(allNodes);

			removeAllIndexes();
			
			tx.success();
		} finally {
			tx.close();
		}
	}

	private Transaction beginTx() {
		return this.graphDatabaseService.beginTx();
	}

	private void removeAllIndexes() {
		
		IndexManager indexManager = this.graphDatabaseService.index();

		deleteNodeIndexes(indexManager);
		deleteRelationshipIndexes(indexManager);
		
	}

	private void deleteRelationshipIndexes(IndexManager indexManager) {
		String[] relationshipIndexNames = indexManager.relationshipIndexNames();
		
		for (String relationshipIndexName : relationshipIndexNames) {
			indexManager.forRelationships(relationshipIndexName).delete();
		}
	}

	private void deleteNodeIndexes(IndexManager indexManager) {
		String[] nodeIndexNames = indexManager.nodeIndexNames();
		
		for (String nodeIndexName : nodeIndexNames) {
			indexManager.forNodes(nodeIndexName).delete();
		}
	}

	private void removeAllNodes(Iterator<Node> allNodes) {
		
		while(allNodes.hasNext()) {
			Node node = allNodes.next();
			node.delete();
		}
	}

	private void removeAllRelationships(Iterator<Relationship> allRelationships) {
		while(allRelationships.hasNext()) {
			allRelationships.next().delete();
		}
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		return compareData(expectedData);
	}

	private boolean compareData(InputStream expectedData) throws NoSqlAssertionError {
		try {
			return executeComparison(new Neo4jConnectionCallback() {
				
				@Override
				public GraphDatabaseService graphDatabaseService() {
					return graphDatabaseService;
				}
			}, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}


	@Override
	public GraphDatabaseService connectionManager() {
		return this.graphDatabaseService;
	}


	public List<Node> getAllNodes() {
		Transaction tx = beginTx();
		try {
			List<Node> result = new ArrayList<Node>(1000);
			Iterator<Node> it = Neo4jLowLevelOps.getAllNodes(graphDatabaseService);
			while (it.hasNext()) {
				result.add(it.next());
			}
			tx.success();
			return result;
		} finally {
			tx.close();
		}
	}
	public List<Relationship> getAllRelationships() {
		Transaction tx = beginTx();
		try {
			List<Relationship> result = new ArrayList<Relationship>(1000);
			Iterator<Relationship> it = Neo4jLowLevelOps.getAllRelationships(graphDatabaseService);
			while (it.hasNext()) {
				result.add(it.next());
			}
			tx.success();
			return result;
		} finally {
			tx.close();
		}
	}

	public String[] nodeIndexNames() {
		Transaction tx = beginTx();
		try {
			String[] names = graphDatabaseService.index().nodeIndexNames();
			tx.success();
			return names;
		} finally {
			tx.close();
		}

	}
}
