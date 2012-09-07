package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.InMemoryNeo4j.InMemoryNeo4jRuleBuilder.newInMemoryNeo4j;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.server.configuration.Configurator;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4jInstances;
import com.lordofthejars.nosqlunit.neo4j.InMemoryNeo4j;

public class WhenInMemoryNeo4jLifecycleIsManaged {

	
	private static final String DEFAULT_NEO4J_TARGET_PATH = InMemoryNeo4j.INMEMORY_NEO4J_TARGET_PATH;
	private static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;
	private static final String LOCALHOST = "127.0.0.1";
	
	
	@Test
	public void neo4j_should_start_in_memory_and_working() throws Throwable {
		
		InMemoryNeo4j inMemoryNeo4j = newInMemoryNeo4j().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
			}
		};
		
		Statement decotedStatement = inMemoryNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), nullValue());
		
	}
	
	@Test
	public void simulataneous_neo4j_should_start_only_one_instance_for_location() throws Throwable {

		InMemoryNeo4j inMemoryNeo4j = newInMemoryNeo4j().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				InMemoryNeo4j defaultInMemoryNeo4j = newInMemoryNeo4j().build();
				
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
					
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
						assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
					}
				};
				
				Statement defaultStatement = defaultInMemoryNeo4j.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
				
			}
		};
		
		Statement decotedStatement = inMemoryNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), nullValue());
		
		
	}
	
}
