package com.lordofthejars.nosqlunit.neo4j;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class Neo4jRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "xml";
	
	private DatabaseOperation databaseOperation;
	
	public Neo4jRule(Neo4jConfiguration neo4jConfiguration) {
		super(neo4jConfiguration.getConnectionIdentifier());
		this.databaseOperation = new Neo4jOperation(neo4jConfiguration.getGraphDatabaseService());
	}

	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public Neo4jRule(Neo4jConfiguration neo4jConfiguration, Object target) {
		super(neo4jConfiguration.getConnectionIdentifier());
		setTarget(target);
		this.databaseOperation = new Neo4jOperation(neo4jConfiguration.getGraphDatabaseService());
	}
	
	@Override
	public DatabaseOperation getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
