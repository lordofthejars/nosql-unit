package com.lordofthejars.nosqlunit.neo4j.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static com.lordofthejars.nosqlunit.neo4j.Neo4jRule.Neo4jRuleBuilder.newNeo4jRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.neo4j.Neo4jRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="embedded-neo4j-spring-definition.xml")
public class WhenSpringEmbeddedInstanceIsRequired {

	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private GraphDatabaseService graphDatabaseService;
	
	@Rule
	public Neo4jRule neo4jRule = newNeo4jRule().defaultSpringGraphDatabaseServiceNeo4j();

	@Test
	public void connection_manager_should_be_the_one_defined_in_application_context() {
		
		DatabaseOperation<GraphDatabaseService> databaseOperation = neo4jRule.getDatabaseOperation();
		GraphDatabaseService connectionManager = databaseOperation.connectionManager();
		
		assertThat(connectionManager, is(graphDatabaseService));
		
	}
	
}
