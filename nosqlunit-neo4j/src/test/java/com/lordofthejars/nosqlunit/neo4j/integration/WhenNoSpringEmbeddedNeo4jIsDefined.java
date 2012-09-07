package com.lordofthejars.nosqlunit.neo4j.integration;

import static com.lordofthejars.nosqlunit.neo4j.SpringEmbeddedNeo4j.SpringEmbeddedNeo4jRuleBuilder.newSpringEmbeddedNeo4jRule;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lordofthejars.nosqlunit.neo4j.SpringEmbeddedNeo4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="/META-INF/spring/applicationContext-nograph.xml")
public class WhenNoSpringEmbeddedNeo4jIsDefined {


	@Autowired
	private ApplicationContext applicationContext;
	
	@Test(expected=IllegalStateException.class)
	public void neo4j_should_throw_an_exception() throws Throwable {
		
		SpringEmbeddedNeo4j springEmbeddedGds = newSpringEmbeddedNeo4jRule().beanFactory(applicationContext).build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
			}
		};
		
		Statement decotedStatement = springEmbeddedGds.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
	}
	
}
