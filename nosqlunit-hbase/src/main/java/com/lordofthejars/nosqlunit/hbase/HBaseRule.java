package com.lordofthejars.nosqlunit.hbase;

import static com.lordofthejars.nosqlunit.hbase.ManagedHBaseConfigurationBuilder.newManagedHBaseConfiguration;
import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBaseConfigurationBuilder.newEmbeddedHBaseConfiguration;

import org.apache.hadoop.conf.Configuration;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class HBaseRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<Configuration> databaseOperation;
	
	public static class HBaseRuleBuilder {
		
		private HBaseConfiguration hBaseConfiguration;
		private Object target;
		
		private HBaseRuleBuilder() {
			
		}
		
		public static HBaseRuleBuilder newHBaseRule() {
			return new HBaseRuleBuilder();
		}
		
		public HBaseRuleBuilder configure(HBaseConfiguration hBaseConfiguration) {
			this.hBaseConfiguration = hBaseConfiguration;
			return this;
		}
		
		public HBaseRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public HBaseRule defaultEmbeddedHBase() {
			return new HBaseRule(newEmbeddedHBaseConfiguration().build());
		}
		
		public HBaseRule defaultEmbeddedHBase(Object target) {
			return new HBaseRule(newEmbeddedHBaseConfiguration().build(), target);
		}
		
		public HBaseRule defaultManagedHBase() {
			return new HBaseRule(newManagedHBaseConfiguration().build());
		}
		
		public HBaseRule defaultManagedHBase(Object target) {
			return new HBaseRule(newManagedHBaseConfiguration().build(), target);
		}
		
		public HBaseRule build() {
			
			if(this.hBaseConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new HBaseRule(hBaseConfiguration, target);
			
		}
		
	}
	
	public HBaseRule(HBaseConfiguration hBaseConfiguration) {
		super(hBaseConfiguration.getConnectionIdentifier());
		this.databaseOperation = new HBaseOperation(hBaseConfiguration.getConfiguration());
	}

	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public HBaseRule(HBaseConfiguration hBaseConfiguration, Object target) {
		super(hBaseConfiguration.getConnectionIdentifier());
		setTarget(target);
		this.databaseOperation = new HBaseOperation(hBaseConfiguration.getConfiguration());
	}
	
	@Override
	public DatabaseOperation<Configuration> getDatabaseOperation() {
		return databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

	
	
}
