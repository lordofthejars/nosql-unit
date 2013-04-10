package com.lordofthejars.nosqlunit.demo.neo4j;

import static com.lordofthejars.nosqlunit.neo4j.InMemoryNeo4j.InMemoryNeo4jRuleBuilder.newInMemoryNeo4j;
import static com.lordofthejars.nosqlunit.neo4j.Neo4jRule.Neo4jRuleBuilder.newNeo4jRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.neo4j.InMemoryNeo4j;
import com.lordofthejars.nosqlunit.neo4j.Neo4jRule;

@UsingDataSet(locations="matrix.xml", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
public class WhenNeoIsRequired {

	@ClassRule
	public static InMemoryNeo4j inMemoryNeo4j = newInMemoryNeo4j().build();
	
	@Rule
	public Neo4jRule neo4jRule = newNeo4jRule().defaultEmbeddedNeo4j(this);
	
	@Inject
	private GraphDatabaseService graphDatabaseService;
	
	
	@Test
	public void neo_node_should_be_returned() {
		MatrixManager matrixManager = new MatrixManager(graphDatabaseService);
		Node neo = matrixManager.getNeoNode();
		assertThat((String)neo.getProperty("name"), is("Thomas Anderson"));
	}
	
	@Test
	public void neo_node_should_be_returnedd() {
		MatrixManager matrixManager = new MatrixManager(graphDatabaseService);
		Node neo = matrixManager.getNeoNode();
		assertThat((String)neo.getProperty("name"), is("Thomas Anderson"));
	}
	
}
