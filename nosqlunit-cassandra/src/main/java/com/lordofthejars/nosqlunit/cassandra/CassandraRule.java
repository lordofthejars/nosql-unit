package com.lordofthejars.nosqlunit.cassandra;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CassandraRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation databaseOperation;

	public CassandraRule(CassandraConfiguration cassandraConfiguration) {
		super(cassandraConfiguration.getConnectionIdentifier());
		this.databaseOperation = new CassandraOperation(cassandraConfiguration);
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public CassandraRule(CassandraConfiguration cassandraConfiguration, Object target) {
		this(cassandraConfiguration);
		setTarget(target);
	}
	
	@Override
	public DatabaseOperation getDatabaseOperation() {
		return databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
