package com.lordofthejars.nosqlunit.demo.neo4j;

import static com.lordofthejars.nosqlunit.neo4j.ManagedWrappingNeoServer.ManagedWrappingNeoServerRuleBuilder.newWrappingNeoServerNeo4jRule;
import static com.lordofthejars.nosqlunit.neo4j.Neo4jRule.Neo4jRuleBuilder.newNeo4jRule;

import javax.inject.Inject;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.neo4j.ManagedWrappingNeoServer;
import com.lordofthejars.nosqlunit.neo4j.Neo4jRule;

public class WhenNeoMeetsANewFriend {

	@ClassRule
	public static ManagedWrappingNeoServer managedWrappingNeoServer = newWrappingNeoServerNeo4jRule().build();
	
	@Rule
	public Neo4jRule neo4jRule = newNeo4jRule().defaultManagedNeo4j(this);
	
	@Inject
	private GraphDatabaseService graphDatabaseService;
	
	@Test
	@UsingDataSet(locations="matrix.xml", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	@ShouldMatchDataSet(location="expected-matrix.xml")
	public void friend_should_be_related_into_neo_graph() {
		
		MatrixManager matrixManager = new MatrixManager(graphDatabaseService);
		matrixManager.addNeoFriend("The Oracle", 4);
	}
	
}
