package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4j.EmbeddedNeo4jRuleBuilder.newEmbeddedNeo4jRule;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.server.configuration.Configurator;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4j;
import com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4jInstances;


public class WhenEmbeddedNeo4jLifecycleIsManaged {

	private static final String DEFAULT_NEO4J_TARGET_PATH = "target" + File.separatorChar + "neo4j-temp";
	private static final String NOT_DEFAULT_LOCATION = "target/neo";
	private static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;
	private static final String LOCALHOST = "127.0.0.1";
	
	@Test
	public void neo4j_should_start_at_default_location() throws Throwable {
		
		EmbeddedNeo4j embeddedNeo4j = newEmbeddedNeo4jRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
			}
		};
		
		Statement decotedStatement = embeddedNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(DEFAULT_NEO4J_TARGET_PATH);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), nullValue());
		
	}
	
	@Test
	public void simulataneous_neo4j_should_start_in_different_locations() throws Throwable {
		
		EmbeddedNeo4j embeddedNeo4j = newEmbeddedNeo4jRule().targetPath(NOT_DEFAULT_LOCATION).build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				EmbeddedNeo4j defaultEmbeddedNeo4j = newEmbeddedNeo4jRule().build();
				
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
					
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+NOT_DEFAULT_LOCATION, PORT), is(true));
						assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(NOT_DEFAULT_LOCATION), notNullValue());
						
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
						assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
					}
				};
				
				Statement defaultStatement = defaultEmbeddedNeo4j.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), nullValue());
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+NOT_DEFAULT_LOCATION, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(NOT_DEFAULT_LOCATION), notNullValue());
			}
		};
		
		Statement decotedStatement = embeddedNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(NOT_DEFAULT_LOCATION);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+NOT_DEFAULT_LOCATION, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(NOT_DEFAULT_LOCATION), nullValue());
		
	}
	
	
	@Test
	public void neo4j_should_start_in_given_location() throws Throwable {
		
		EmbeddedNeo4j embeddedNeo4j = newEmbeddedNeo4jRule().targetPath(NOT_DEFAULT_LOCATION).build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+NOT_DEFAULT_LOCATION, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(NOT_DEFAULT_LOCATION), notNullValue());
			}
		};
		
		Statement decotedStatement = embeddedNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(NOT_DEFAULT_LOCATION);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+NOT_DEFAULT_LOCATION, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(NOT_DEFAULT_LOCATION), nullValue());
		
	}
	
	@Test
	public void simulataneous_neo4j_should_start_only_one_instance_for_location() throws Throwable {

		EmbeddedNeo4j embeddedNeo4j = newEmbeddedNeo4jRule().build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				EmbeddedNeo4j defaultEmbeddedNeo4j = newEmbeddedNeo4jRule().build();
				
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
					
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
						assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
					}
				};
				
				Statement defaultStatement = defaultEmbeddedNeo4j.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), notNullValue());
				
			}
		};
		
		Statement decotedStatement = embeddedNeo4j.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File(DEFAULT_NEO4J_TARGET_PATH);
		assertThat(dbPath.exists(), is(false));
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(DEFAULT_NEO4J_TARGET_PATH), nullValue());
		
		
	}
		

	
}
