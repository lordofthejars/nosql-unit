package com.lordofthejars.nosqlunit.cassandra;

import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandraConfigurationBuilder.newEmbeddedCassandraConfiguration;
import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandraConfigurationBuilder.newManagedCassandraConfiguration;

import me.prettyprint.hector.api.Keyspace;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CassandraRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<Keyspace> databaseOperation;

	public static class CassandraRuleBuilder {
		
		private CassandraConfiguration cassandraConfiguration;
		private Object target;
		
		private CassandraRuleBuilder() {
			super();
		}
		
		public static CassandraRuleBuilder newCassandraRule() {
			return new CassandraRuleBuilder();
		}
		
		public CassandraRuleBuilder configure(CassandraConfiguration cassandraConfiguration) {
			this.cassandraConfiguration = cassandraConfiguration;
			return this;
		}
		
		public CassandraRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public CassandraRule defaultEmbeddedCassandra(String clusterName) {
			return new CassandraRule(newEmbeddedCassandraConfiguration().clusterName(clusterName).build());
		}
		
		public CassandraRule defaultEmbeddedCassandra(String clusterName, Object target) {
			return new CassandraRule(newEmbeddedCassandraConfiguration().clusterName(clusterName).build(), target);
		}
		
		public CassandraRule defaultManagedCassandra(String clusterName) {
			return new CassandraRule(newManagedCassandraConfiguration().clusterName(clusterName).build());
		}
		
		public CassandraRule defaultManagedCassandra(String clusterName, Object target) {
			return new CassandraRule(newManagedCassandraConfiguration().clusterName(clusterName).build(), target);
		}
		
		public CassandraRule build() {

			if(this.cassandraConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new CassandraRule(cassandraConfiguration, target);
		}
		
	}
	
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
	public DatabaseOperation<Keyspace> getDatabaseOperation() {
		return databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
