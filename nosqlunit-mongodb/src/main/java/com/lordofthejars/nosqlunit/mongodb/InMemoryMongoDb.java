package com.lordofthejars.nosqlunit.mongodb;

import org.junit.rules.ExternalResource;

public class InMemoryMongoDb extends ExternalResource {

	protected InMemoryMongoDbLifecycleManager inMemoryMongoDbLifecycleManager = null;

	private InMemoryMongoDb() {
		super();
	}
	
	public static class InMemoryMongoRuleBuilder {
		
		private InMemoryMongoDbLifecycleManager inMemoryMongoDbLifecycleManager;
		
		private InMemoryMongoRuleBuilder() {
			this.inMemoryMongoDbLifecycleManager = new InMemoryMongoDbLifecycleManager();
		}
		
		public static InMemoryMongoRuleBuilder newInMemoryMongoDbRule() {
			return new InMemoryMongoRuleBuilder();
		}
		
		public InMemoryMongoRuleBuilder targetPath(String targetPath) {
			this.inMemoryMongoDbLifecycleManager.setTargetPath(targetPath);
			return this;
		}
	
		
		public InMemoryMongoDb build() {
			
			if(this.inMemoryMongoDbLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Infinispan is provided.");
			}
			
			InMemoryMongoDb inMemoryMongoDb = new InMemoryMongoDb();
			inMemoryMongoDb.inMemoryMongoDbLifecycleManager = this.inMemoryMongoDbLifecycleManager;
			
			return inMemoryMongoDb;
			
		}
		
	}
	
	@Override
	public void before() throws Throwable {
		inMemoryMongoDbLifecycleManager.startEngine();
	}

	@Override
	public void after() {
		inMemoryMongoDbLifecycleManager.stopEngine();
	}
	

}
