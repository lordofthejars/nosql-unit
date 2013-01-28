package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.EmbeddedInfinispanConfigurationBuilder.newEmbeddedInfinispanConfiguration;
import static com.lordofthejars.nosqlunit.infinispan.ManagedInfinispanConfigurationBuilder.newManagedInfinispanConfiguration;

import org.infinispan.api.BasicCache;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class InfinispanRule extends AbstractNoSqlTestRule {

	private final static String EXTENSION = "json";
	
	private DatabaseOperation<BasicCache<Object, Object>> databaseOperation;
	
	public static class InfinispanRuleBuilder {
		
		private InfinispanConfiguration infinispanConfiguration;
		private Object target;
		
		private InfinispanRuleBuilder() {
			super();
		}
		
		public static InfinispanRuleBuilder newInfinispanRule() {
			return new InfinispanRuleBuilder();
		}
		
		public InfinispanRuleBuilder configure(InfinispanConfiguration infinispanConfiguration) {
			this.infinispanConfiguration = infinispanConfiguration;
			return this;
		}
		
		public InfinispanRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public InfinispanRule defaultEmbeddedInfinispan() {
			return new InfinispanRule(newEmbeddedInfinispanConfiguration().build());
		}
		
		/**
		 * We can use defaultEmbeddedInfinispan().
		 * @param target
		 * @return
		 */
		@Deprecated
		public InfinispanRule defaultEmbeddedInfinispan(Object target) {
			return new InfinispanRule(newEmbeddedInfinispanConfiguration().build(), target);
		}
		
		public InfinispanRule defaultManagedInfinispan() {
			return new InfinispanRule(newManagedInfinispanConfiguration().build());
		}
		
		public InfinispanRule defaultManagedInfinispan(int port) {
			return new InfinispanRule(newManagedInfinispanConfiguration().port(port).build());
		}
		
		/**
		 * We can use defaultManagedInfinispan().
		 * @param target
		 * @return
		 */
		@Deprecated
		public InfinispanRule defaultManagedInfinispan(Object target) {
			return new InfinispanRule(newManagedInfinispanConfiguration().build(), target);
		}
		
		public InfinispanRule build() {
			
			if(this.infinispanConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new InfinispanRule(infinispanConfiguration, target);
		}
		
	}
	
	public InfinispanRule(InfinispanConfiguration infinispanConfiguration) {
		super(infinispanConfiguration.getConnectionIdentifier());
		this.databaseOperation = new InfinispanOperation(infinispanConfiguration.getCache());
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public InfinispanRule(InfinispanConfiguration infinispanConfiguration, Object target) {
		this(infinispanConfiguration);
		setTarget(target);
	}

	@Override
	public DatabaseOperation<BasicCache<Object, Object>> getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
