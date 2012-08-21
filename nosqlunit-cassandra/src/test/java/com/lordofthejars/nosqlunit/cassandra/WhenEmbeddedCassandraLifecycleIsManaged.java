package com.lordofthejars.nosqlunit.cassandra;

import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandra.EmbeddedCassandraRuleBuilder.newEmbeddedCassandraRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;


public class WhenEmbeddedCassandraLifecycleIsManaged {

	@Mock
	private EmbeddedCassandraServerHelper embeddedCassandraServerHelper;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void embedded_cassandra_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		EmbeddedCassandra cassandraRule = newEmbeddedCassandraRule().build();
		cassandraRule.setEmbeddedCassandraServerHelper(embeddedCassandraServerHelper);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(true));
			}
		};
		
		Statement decotedStatement = cassandraRule.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		verify(embeddedCassandraServerHelper).startEmbeddedCassandra(EmbeddedCassandra.DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION, EmbeddedCassandra.DEFAULT_CASSANDRA_TARGET_PATH);
		verify(embeddedCassandraServerHelper).stopEmbeddedCassandra();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(false));
		
	}
	
	@Test
	public void embedded_cassandra_should_be_registered_and_started_with_custom_parameters() throws Throwable {
		
		EmbeddedCassandra cassandraRule = newEmbeddedCassandraRule().targetPath("tmp").cassandraConfigurationPath("my_cassandra.yaml").build();
		cassandraRule.setEmbeddedCassandraServerHelper(embeddedCassandraServerHelper);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(true));
			}
		};
		
		Statement decotedStatement = cassandraRule.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		verify(embeddedCassandraServerHelper).startEmbeddedCassandra("my_cassandra.yaml", "tmp");
		verify(embeddedCassandraServerHelper).stopEmbeddedCassandra();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(false));
		
	}
	
	@Test
	public void simulataneous_cassandra_should_start_only_one_instance() throws Throwable {

		EmbeddedCassandra cassandraRule = newEmbeddedCassandraRule().build();
		cassandraRule.setEmbeddedCassandraServerHelper(embeddedCassandraServerHelper);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				EmbeddedCassandra defaultEmbeddedCassandra =newEmbeddedCassandraRule().build();
				defaultEmbeddedCassandra.setEmbeddedCassandraServerHelper(embeddedCassandraServerHelper);
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(true));
					}
				};
				
				Statement defaultStatement = defaultEmbeddedCassandra.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(true));
				
			}
		};
		
		Statement decotedStatement = cassandraRule.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		verify(embeddedCassandraServerHelper).startEmbeddedCassandra(EmbeddedCassandra.DEFAULT_CASSANDRA_CONFIGURATION_FILE_LOCATION, EmbeddedCassandra.DEFAULT_CASSANDRA_TARGET_PATH);
		verify(embeddedCassandraServerHelper).stopEmbeddedCassandra();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(EmbeddedCassandra.LOCALHOST, EmbeddedCassandra.PORT), is(false));
		
		
	}
	
}
