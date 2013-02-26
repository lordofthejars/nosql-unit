package com.lordofthejars.nosqlunit.neo4j;

import static ch.lambdaj.Lambda.selectFirst;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.collection.LambdaCollections.with;
import static org.hamcrest.CoreMatchers.anything;

import java.util.Map;

import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.context.ApplicationContext;

import com.lordofthejars.nosqlunit.core.PropertyGetter;

class SpringGraphDatabaseServiceNeo4jRule extends Neo4jRule {

	private PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<ApplicationContext>();

	public SpringGraphDatabaseServiceNeo4jRule(Neo4jConfiguration neo4jConfiguration) {
		super(neo4jConfiguration);
	}

	public SpringGraphDatabaseServiceNeo4jRule(Neo4jConfiguration neo4jConfiguration, Object object) {
		super(neo4jConfiguration, object);
	}

	@Override
	public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
		this.databaseOperation = new Neo4jOperation(definedGraphDatabaseService(testObject));
		return super.apply(base, method, testObject);
	}

	private GraphDatabaseService definedGraphDatabaseService(Object testObject) {
		ApplicationContext applicationContext = propertyGetter.propertyByType(testObject, ApplicationContext.class);

		Map<String, GraphDatabaseService> beansOfType = applicationContext.getBeansOfType(GraphDatabaseService.class);
		GraphDatabaseService graphDatabaseService = with(beansOfType).values().first(anything());

		if (graphDatabaseService == null) {
			throw new IllegalArgumentException(
					"At least one GraphDatabaseService instance should be defined into Spring Application Context.");
		}

		return graphDatabaseService;

	}

}
