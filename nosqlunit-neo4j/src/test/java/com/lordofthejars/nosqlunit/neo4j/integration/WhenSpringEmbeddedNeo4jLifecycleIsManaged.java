package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.SpringEmbeddedNeo4j.SpringEmbeddedNeo4jRuleBuilder.newSpringEmbeddedNeo4jRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.neo4j.server.configuration.Configurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4jInstances;
import com.lordofthejars.nosqlunit.neo4j.SpringEmbeddedNeo4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="/META-INF/spring/applicationContext-graph.xml")
public class WhenSpringEmbeddedNeo4jLifecycleIsManaged {

	private static final String SPRING_TARGET_PATH_DEFAULT_NEO4J_TARGET_PATH = "target/config-test";
	private static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;
	private static final String LOCALHOST = "127.0.0.1";
	

	@Autowired
	private ApplicationContext applicationContext;
	
	@Test
	public void neo4j_should_use_instance_from_spring_application_context() throws Throwable {
		
		SpringEmbeddedNeo4j springEmbeddedGds = newSpringEmbeddedNeo4jRule().beanFactory(applicationContext).build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+SPRING_TARGET_PATH_DEFAULT_NEO4J_TARGET_PATH, PORT), is(true));
				assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(SPRING_TARGET_PATH_DEFAULT_NEO4J_TARGET_PATH), notNullValue());
			}
		};
		
		Statement decotedStatement = springEmbeddedGds.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST+SPRING_TARGET_PATH_DEFAULT_NEO4J_TARGET_PATH, PORT), is(false));
		assertThat(EmbeddedNeo4jInstances.getInstance().getGraphDatabaseServiceByTargetPath(SPRING_TARGET_PATH_DEFAULT_NEO4J_TARGET_PATH), nullValue());
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void neo4j_should_throw_exception_if_no_bean_factory_is_provided() {
		SpringEmbeddedNeo4j springEmbeddedGds = newSpringEmbeddedNeo4jRule().build();
	}
	
}
