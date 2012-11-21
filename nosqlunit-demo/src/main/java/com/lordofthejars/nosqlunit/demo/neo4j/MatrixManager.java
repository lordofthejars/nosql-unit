package com.lordofthejars.nosqlunit.demo.neo4j;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

public class MatrixManager {

	public enum RelTypes implements RelationshipType {
		NEO_NODE, KNOWS, CODED_BY
	}

	private GraphDatabaseService graphDb;

	public MatrixManager(GraphDatabaseService graphDatabaseService) {
		this.graphDb = graphDatabaseService;
	}

	public int countNeoFriends() {

		Node neoNode = getNeoNode();
		Traverser friendsTraverser = getFriends(neoNode);

		return friendsTraverser.getAllNodes().size();

	}

	public void addNeoFriend(String name, int age) {
		Transaction tx = this.graphDb.beginTx();
		try {
			Node friend = this.graphDb.createNode();
			friend.setProperty("name", name);
			Relationship relationship = getNeoNode().createRelationshipTo(friend, RelTypes.KNOWS);
			relationship.setProperty("age", age);
			tx.success();
		} finally {
			tx.finish();
		}
	}

	private Traverser getFriends(final Node person) {
		return person.traverse(Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
				RelTypes.KNOWS, Direction.OUTGOING);
	}

	public Node getNeoNode() {
		return graphDb.getReferenceNode().getSingleRelationship(RelTypes.NEO_NODE, Direction.OUTGOING).getEndNode();
	}

}
