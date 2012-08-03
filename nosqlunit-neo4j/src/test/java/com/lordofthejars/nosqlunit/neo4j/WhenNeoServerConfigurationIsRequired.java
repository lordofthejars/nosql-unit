package com.lordofthejars.nosqlunit.neo4j;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static com.lordofthejars.nosqlunit.neo4j.EmbeddedNeoServerConfigurationBuilder.newEmbeddedNeoServerConfiguration;
import static com.lordofthejars.nosqlunit.neo4j.ManagedNeoServerConfigurationBuilder.newManagedNeoServerConfiguration;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.graphdb.RestGraphDatabase;

public class WhenNeoServerConfigurationIsRequired {

	@Test
	public void in_memory_configuration_should_use_default_embedded_instance() {
		
		GraphDatabaseService graphDatabaseService = mock(GraphDatabaseService.class);
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDatabaseService, "a");
		
		EmbeddedNeoServerConfigurationBuilder embeddedNeoServerConfiguration = newEmbeddedNeoServerConfiguration();
		Neo4jConfiguration embeddedConfiguration = embeddedNeoServerConfiguration.build();
		EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService("a");
		
		assertThat(embeddedConfiguration.getGraphDatabaseService(), is(graphDatabaseService));
		
	}

	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_default_embedded() {
		
		
		EmbeddedNeoServerConfigurationBuilder embeddedNeoServerConfiguration = newEmbeddedNeoServerConfiguration();
		Neo4jConfiguration embeddedConfiguration = embeddedNeoServerConfiguration.build();
		
	}

	@Test
	public void in_memory_configuration_should_use_targeted_instance() {
		
		GraphDatabaseService graphDatabaseService1 = mock(GraphDatabaseService.class);
		GraphDatabaseService graphDatabaseService2 = mock(GraphDatabaseService.class);
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDatabaseService1, "a");
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDatabaseService2, "b");
		
		EmbeddedNeoServerConfigurationBuilder embeddedNeoServerConfiguration = newEmbeddedNeoServerConfiguration();
		Neo4jConfiguration embeddedConfiguration = embeddedNeoServerConfiguration.buildFromTargetPath("b");
		EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService("a");
		EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService("b");
		
		assertThat(embeddedConfiguration.getGraphDatabaseService(), is(graphDatabaseService2));
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_targeted_instance() {
		
		EmbeddedNeoServerConfigurationBuilder embeddedNeoServerConfiguration = newEmbeddedNeoServerConfiguration();
		Neo4jConfiguration embeddedConfiguration = embeddedNeoServerConfiguration.buildFromTargetPath("a");
		
	}
	
	@Test
	public void managed_configuration_should_create_a_rest_connection() {
		
		Neo4jConfiguration managedNeoServer = newManagedNeoServerConfiguration().password("alex").username("alex").uri("http://localhost").build();
		
		assertThat(managedNeoServer.getUserName(), is("alex"));
		assertThat(managedNeoServer.getPassword(), is("alex"));
		assertThat(managedNeoServer.getUri(), is("http://localhost"));
		assertThat(managedNeoServer.getGraphDatabaseService(), instanceOf(RestGraphDatabase.class));
		
	}
	
	@Test
	public void managed_configuration_should_create_a_rest_connection_with_default_uri() {
		
		Neo4jConfiguration managedNeoServer = newManagedNeoServerConfiguration().password("alex").username("alex").build();
		
		assertThat(managedNeoServer.getUserName(), is("alex"));
		assertThat(managedNeoServer.getPassword(), is("alex"));
		assertThat(managedNeoServer.getUri(), is(Neo4jConfiguration.DEFAULT_URI));
		assertThat(managedNeoServer.getGraphDatabaseService(), instanceOf(RestGraphDatabase.class));
		
	}
	
}
