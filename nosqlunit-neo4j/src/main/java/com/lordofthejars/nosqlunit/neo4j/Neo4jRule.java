package com.lordofthejars.nosqlunit.neo4j;

import static com.lordofthejars.nosqlunit.neo4j.ManagedNeoServerConfigurationBuilder.newManagedNeoServerConfiguration;
import static com.lordofthejars.nosqlunit.neo4j.EmbeddedNeoServerConfigurationBuilder.newEmbeddedNeoServerConfiguration;

import org.neo4j.graphdb.GraphDatabaseService;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class Neo4jRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "xml";
	
	protected DatabaseOperation<GraphDatabaseService> databaseOperation;
	
	public static class Neo4jRuleBuilder {
		
		private Neo4jConfiguration neo4jConfiguration;
		private Object target;
		
		private Neo4jRuleBuilder() {
			
		}
		
		public static Neo4jRuleBuilder newNeo4jRule() {
			return new Neo4jRuleBuilder();
		}
		
		public Neo4jRuleBuilder configure(Neo4jConfiguration neo4jConfiguration) {
			this.neo4jConfiguration = neo4jConfiguration;
			return this;
		}
		
		public Neo4jRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public Neo4jRule defaultSpringGraphDatabaseServiceNeo4j(Neo4jConfiguration neo4jConfiguration) {
            return new SpringGraphDatabaseServiceNeo4jRule(neo4jConfiguration);
        }
		
		public Neo4jRule defaultSpringGraphDatabaseServiceNeo4j() {
			return new SpringGraphDatabaseServiceNeo4jRule(new Neo4jConfiguration());
		}
		
		public Neo4jRule defaultEmbeddedNeo4j() {
			return new Neo4jRule(newEmbeddedNeoServerConfiguration().build());
		}
		
		/**
		 * We can use defaultEmbeddedNeo4j().
		 * @param target
		 * @return
		 */
		@Deprecated
		public Neo4jRule defaultEmbeddedNeo4j(Object target) {
			return new Neo4jRule(newEmbeddedNeoServerConfiguration().build(), target);
		}
		
		public Neo4jRule defaultManagedNeo4j() {
			return new Neo4jRule(newManagedNeoServerConfiguration().build());
		}
		
		public Neo4jRule defaultManagedNeo4j(String uri) {
			return new Neo4jRule(newManagedNeoServerConfiguration().uri(uri).build());
		}
		
		/**
		 * We can use defaultManagedNeo4j().
		 * @param target
		 * @return
		 */
		@Deprecated
		public Neo4jRule defaultManagedNeo4j(Object target) {
			return new Neo4jRule(newManagedNeoServerConfiguration().build(), target);
		}
		
		public Neo4jRule build() {
			
			if(this.neo4jConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new Neo4jRule(neo4jConfiguration, target);
			
		}
		
	}
	
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
	public DatabaseOperation<GraphDatabaseService> getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
