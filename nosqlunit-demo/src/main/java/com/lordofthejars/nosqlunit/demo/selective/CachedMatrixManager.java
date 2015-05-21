package com.lordofthejars.nosqlunit.demo.selective;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

import org.neo4j.tooling.GlobalGraphOperations;
import redis.clients.jedis.Jedis;

public class CachedMatrixManager {

	private static final String NEO_KEY = "Neo";

	public enum RelTypes implements RelationshipType {
		NEO_NODE, KNOWS, CODED_BY
	}

	private GraphDatabaseService graphDb;
	private Jedis jedis;

	public CachedMatrixManager(GraphDatabaseService graphDatabaseService, Jedis jedis) {
		this.graphDb = graphDatabaseService;
		this.jedis = jedis;
	}

	public int countNeoFriends() {

		Boolean isNumberOfNeosFriendsCached = jedis.exists(NEO_KEY);
		
		if (isNumberOfNeosFriendsCached) {
			
			String numberOfFriends = jedis.get(NEO_KEY);
			return Integer.parseInt(numberOfFriends);
			
		} else {

			Node neoNode = getNeoNode();
			Traverser friendsTraverser = getFriends(neoNode);

			int numberOfFriends = friendsTraverser.getAllNodes().size();
			this.jedis.set(NEO_KEY, Integer.toString(numberOfFriends));
			
			return numberOfFriends;
		}

	}

	private static Traverser getFriends(final Node person) {
		return person.traverse(Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
				RelTypes.KNOWS, Direction.OUTGOING);
	}

	public Node getNeoNode() {
		Iterable<Node> allNodes = GlobalGraphOperations.at(graphDb).getAllNodes();
		for(Node node: allNodes) {
			if("Thomas Anderson".equals(node.getProperty("name"))) {
				return node;
			}
		}
		return null;
	}

}
