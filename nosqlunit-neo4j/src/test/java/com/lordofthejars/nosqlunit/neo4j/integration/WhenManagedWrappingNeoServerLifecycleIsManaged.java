package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.ManagedWrappingNeoServer.ManagedWrappingNeoServerRuleBuilder.newWrappingNeoServerNeo4jRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.server.configuration.Configurator;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.neo4j.ManagedWrappingNeoServer;

public class WhenManagedWrappingNeoServerLifecycleIsManaged {

	private static final String LOCALHOST = "127.0.0.1";
	private static final String DEFAULT_NEO4J_TARGET_PATH = "target" + File.separatorChar + "neo4j-temp";
	private static final String NOT_DEFAULT_LOCATION = "target/neo";
	private static final int ALTERNATIVE_PORT = 7575;
	
	@Test
	@Ignore
	public void neo4j_should_start_at_default_location() throws Throwable {
		
		ManagedWrappingNeoServer managedWrappingNeoServer = newWrappingNeoServerNeo4jRule().build();
		
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
		
		Statement decotedStatement = managedWrappingNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(DEFAULT_NEO4J_TARGET_PATH);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(false));
		
	}
	
	@Test
	public void neo4j_should_start_in_different_location_and_port() throws Throwable {
		
		ManagedWrappingNeoServer managedWrappingNeoServer = newWrappingNeoServerNeo4jRule().targetPath(NOT_DEFAULT_LOCATION).port(ALTERNATIVE_PORT).build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				GraphDatabaseService gds = new RestGraphDatabase("http://localhost:"+ALTERNATIVE_PORT+"/db/data");
				long id = Long.MIN_VALUE;
				id = insertNode(gds);
				
				assertThat(id, is(not(Long.MIN_VALUE)));
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, ALTERNATIVE_PORT), is(true));
			}
		};
		
		Statement decotedStatement = managedWrappingNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(DEFAULT_NEO4J_TARGET_PATH);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, ALTERNATIVE_PORT), is(false));
		
	}
	
	
	@Test
	public void simulataneous_neo4j_should_start_only_one_instance_for_location() throws Throwable {
		
		ManagedWrappingNeoServer managedWrappingNeoServer = newWrappingNeoServerNeo4jRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				ManagedWrappingNeoServer managedWrappingNeoServer2 = newWrappingNeoServerNeo4jRule().build();
				
				Statement noStatement2 = new Statement() {

					@Override
					public void evaluate() throws Throwable {
						
						GraphDatabaseService gds = new RestGraphDatabase("http://localhost:7474/db/data");
						long id = Long.MIN_VALUE;
						id = insertNode(gds);
						
						assertThat(id, is(not(Long.MIN_VALUE)));
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(true));
						
					}
					
				};
				
				Statement secondStatement = managedWrappingNeoServer2.apply(noStatement2, Description.EMPTY);
				secondStatement.evaluate();
				
				GraphDatabaseService gds = new RestGraphDatabase("http://localhost:7474/db/data");
				long id = Long.MIN_VALUE;
				id = insertNode(gds);
				
				assertThat(id, is(not(Long.MIN_VALUE)));
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(true));
			}
		};
		
		Statement decotedStatement = managedWrappingNeoServer.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(DEFAULT_NEO4J_TARGET_PATH);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, Configurator.DEFAULT_WEBSERVER_PORT), is(false));
		
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
