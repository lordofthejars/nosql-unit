package com.lordofthejars.nosqlunit.neo4j.integration;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.tooling.GlobalGraphOperations;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.neo4j.Neo4jLowLevelOps;
import com.lordofthejars.nosqlunit.neo4j.Neo4jOperation;

public class WhenNeo4jOperationsAreRequired {

	private static final String DB_PATH = "target/neo4j-test";
	
	private static final String WELL_FORMED_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String EXPECTED_GRAPH = "<?xml version=\"1.0\" ?>\n" + 
			"<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd\">\n" + 
			"<key id=\"name\" for=\"edge\" attr.name=\"name\" attr.type=\"string\"></key>\n" + 
			"<key id=\"message\" for=\"node\" attr.name=\"message\" attr.type=\"string\"></key>\n" + 
			"	<graph id=\"G\" edgedefault=\"directed\">\n" + 
			"		<node id=\"0\"></node>\n" + 
			"		<node id=\"1\">\n" + 
			"			<data key=\"message\">Hello</data>\n" + 
			"		</node>\n" + 
			"		<node id=\"2\">\n" + 
			"			<data key=\"message\">World!</data>\n" + 
			"		</node>\n" + 
			"		<edge id=\"0\" source=\"1\" target=\"2\" label=\"KNOWS\">\n" + 
			"			<data key=\"name\">Alex</data>\n" + 
			"		</edge>\n" + 
			"	</graph>\n" + 
			"</graphml>";
	
	private static final String EXPECTED_GRAPH_WITHOUT_REFERENCE = "<?xml version=\"1.0\" ?>\n" + 
			"<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd\">\n" + 
			"<key id=\"name\" for=\"edge\" attr.name=\"name\" attr.type=\"string\"></key>\n" + 
			"<key id=\"message\" for=\"node\" attr.name=\"message\" attr.type=\"string\"></key>\n" + 
			"	<graph id=\"G\" edgedefault=\"directed\">\n" + 
			"		<node id=\"1\">\n" + 
			"			<data key=\"message\">Hello</data>\n" + 
			"		</node>\n" + 
			"		<node id=\"2\">\n" + 
			"			<data key=\"message\">World!</data>\n" + 
			"		</node>\n" + 
			"		<edge id=\"0\" source=\"1\" target=\"2\" label=\"KNOWS\">\n" + 
			"			<data key=\"name\">Alex</data>\n" + 
			"		</edge>\n" + 
			"	</graph>\n" + 
			"</graphml>";
	
	@Before
	public void setUp() {
		clearDb();
	}
	
	@Test
	public void insert_opertation_should_add_data_into_graph()  {
		
		GraphDatabaseService newEmbeddedDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		Neo4jOperation neo4jOperation = new Neo4jOperation(newEmbeddedDatabase);
		
		neo4jOperation.insert(new ByteArrayInputStream(WELL_FORMED_GRAPH.getBytes()));
		
		GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(newEmbeddedDatabase);
		Iterable<Node> allNodes = globalGraphOperations.getAllNodes();
		Iterable<Relationship> allRelationships = globalGraphOperations.getAllRelationships();
		
		Node firstNode = selectFirst(allNodes, having(on(Node.class).getProperty("name"), equalTo("I")));
		assertThat(firstNode, notNullValue());
		
		Node secondNode = selectFirst(allNodes, having(on(Node.class).getProperty("name"), equalTo("you")));
		assertThat(secondNode, notNullValue());
		
		Node thirdNode = selectFirst(allNodes, having(on(Node.class).getProperty("name"), equalTo("him")));
		assertThat(thirdNode, notNullValue());
		
		Relationship firstRelationship = selectFirst(allRelationships, having(on(Relationship.class).getProperty("weight"), equalTo(Float.parseFloat("0.5"))));
		assertThat(firstRelationship, notNullValue());
		
		Relationship secondRelationship = selectFirst(allRelationships, having(on(Relationship.class).getProperty("weight"), equalTo(Float.parseFloat("0.8"))));
		assertThat(secondRelationship, notNullValue());
		
		
		newEmbeddedDatabase.shutdown();
	}
	
	
	@Test
	public void delete_all_operation_should_remove_all_data_from_graph_except_reference_node() {
		
		GraphDatabaseService newEmbeddedDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		Neo4jOperation neo4jOperation = new Neo4jOperation(newEmbeddedDatabase);
		
		createNodes(newEmbeddedDatabase);
		
		
		neo4jOperation.deleteAll();
		
		GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(newEmbeddedDatabase);
		
		Iterator<Node> allNodes = globalGraphOperations.getAllNodes().iterator();
		Iterator<Relationship> allRelationships = globalGraphOperations.getAllRelationships().iterator();
		Node referenceNode = allNodes.next();
		assertThat(referenceNode.getId(), is(0L));
		assertThat(allNodes.hasNext(), is(false));
		assertThat(allRelationships.hasNext(), is(false));
		
		newEmbeddedDatabase.shutdown();
		
	}
	
	@Test
	public void delete_all_operation_should_remove_all_data_from_graph_except_reference_node_in_remote_mode() {
		
		GraphDatabaseService newEmbeddedDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		WrappingNeoServerBootstrapper graphDb = new WrappingNeoServerBootstrapper((GraphDatabaseAPI) newEmbeddedDatabase);
		graphDb.start();
		
		RestGraphDatabase graphDatabaseService = new RestGraphDatabase("http://localhost:7474/db/data");
		Neo4jOperation neo4jOperation = new Neo4jOperation(graphDatabaseService);
		
		createNodes(graphDatabaseService);
		
		
		neo4jOperation.deleteAll();
		
		
		Iterator<Node> allNodes = Neo4jLowLevelOps.getAllNodes(graphDatabaseService);
		Iterator<Relationship> allRelationships = Neo4jLowLevelOps.getAllRelationships(graphDatabaseService);
		Node referenceNode = allNodes.next();
		assertThat(referenceNode.getId(), is(0L));
		assertThat(allNodes.hasNext(), is(false));
		assertThat(allRelationships.hasNext(), is(false));
		
		graphDb.stop();
	}

	@Test
	public void should_assert_same_expected_data_and_inserted_data() {
		
		GraphDatabaseService newEmbeddedDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		Neo4jOperation neo4jOperation = new Neo4jOperation(newEmbeddedDatabase);
		
		createNodes(newEmbeddedDatabase);
		
		boolean expectedDataEqualsToInserted = neo4jOperation.databaseIs(new ByteArrayInputStream(EXPECTED_GRAPH.getBytes()));
		
		assertThat(expectedDataEqualsToInserted, is(true));
		
	}
	
	@Test(expected=NoSqlAssertionError.class)
	public void should_throw_exception_same_expected_data_and_inserted_data() {
		
		GraphDatabaseService newEmbeddedDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		Neo4jOperation neo4jOperation = new Neo4jOperation(newEmbeddedDatabase);
		
		createNodes(newEmbeddedDatabase);
		
		neo4jOperation.databaseIs(new ByteArrayInputStream(EXPECTED_GRAPH_WITHOUT_REFERENCE.getBytes()));
	}
	
	private void createNodes(GraphDatabaseService newEmbeddedDatabase) {
		Transaction tx = newEmbeddedDatabase.beginTx();
		try {
			Node firstNode = newEmbeddedDatabase.createNode();
			firstNode.setProperty("message", "Hello");
			Node secondNode = newEmbeddedDatabase.createNode();
			secondNode.setProperty("message", "World!");
			
			Relationship relationship = firstNode.createRelationshipTo(
					secondNode, DynamicRelationshipType.withName("KNOWS"));
			relationship.setProperty("name", "Alex");
			
			tx.success();
		} finally {
			tx.finish();
		}
	}
	
	private void clearDb() {

		try {
			FileUtils.deleteRecursively(new File(DB_PATH));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
}
