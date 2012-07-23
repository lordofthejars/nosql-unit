package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.ManagedNeoServer.Neo4jServerRuleBuilder.newManagedNeo4jServerRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;


import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.server.configuration.Configurator;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.neo4j.ManagedNeoServer;

public class WhenManagedNeoServerLifecycleIsManaged {

	private static final String LOCALHOST = "127.0.0.1";
	
	@Test
	public void neo4j_should_start_and_stop_from_configured_location() throws Throwable {
		
		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().neo4jPath("/opt/neo4j-community-1.7.2").build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				GraphDatabaseService gds = new RestGraphDatabase("http://localhost:7474/db/data");
				long id = Long.MIN_VALUE;
				id = insertNode(gds);
				
				assertThat(id, is(not(Long.MIN_VALUE)));
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(true));
			}
		};
		
		Statement decotedStatement = managedNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(false));
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void neo4j_server_should_throw_an_exception_if_neo4j_location_is_not_set() throws Throwable {
		
		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		Statement decotedStatement = managedNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void neo4j_server_should_throw_an_exception_if_neo4j_location_is_not_found() throws Throwable {
		
		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		Statement decotedStatement = managedNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
	}
	
	private long insertNode(GraphDatabaseService graphDatabaseService) {
		Transaction tx = null;
		try {
			tx = graphDatabaseService.beginTx();
		
			Node node = graphDatabaseService.createNode();
			return node.getId();
			
		} finally {
			tx.success();
			tx.finish();
		}
	}
	
}
