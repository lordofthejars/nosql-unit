package com.lordofthejars.nosqlunit.infinispan;

import org.junit.rules.ExternalResource;

public class EmbeddedInfinispan extends ExternalResource {

	private EmbeddedInfinispan() {
		super();
	}
	
	protected EmbeddedInfinispanLifecycleManager embeddedInfinispanLifecycleManager;
	
	public static class EmbeddedInfinispanRuleBuilder {
		
		private EmbeddedInfinispanLifecycleManager embeddedInfinispanLifecycleManager;
		
		private EmbeddedInfinispanRuleBuilder() {
			this.embeddedInfinispanLifecycleManager = new EmbeddedInfinispanLifecycleManager();
		}
		
		public static EmbeddedInfinispanRuleBuilder newEmbeddedInfinispanRule() {
			return new EmbeddedInfinispanRuleBuilder();
		}
		
		public EmbeddedInfinispanRuleBuilder targetPath(String targetPath) {
			this.embeddedInfinispanLifecycleManager.setTargetPath(targetPath);
			return this;
		}
		
		public EmbeddedInfinispanRuleBuilder configurationFile(String configurationFile) {
			this.embeddedInfinispanLifecycleManager.setConfigurationFile(configurationFile);
			return this;
		}
		
		public EmbeddedInfinispan build() {
			
			if(this.embeddedInfinispanLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Infinispan is provided.");
			}
			
			EmbeddedInfinispan embeddedInfinispan = new EmbeddedInfinispan();
			embeddedInfinispan.embeddedInfinispanLifecycleManager = this.embeddedInfinispanLifecycleManager;
			
			return embeddedInfinispan;
			
		}
		
	}
	
	@Override
	protected void before() throws Throwable {
		this.embeddedInfinispanLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.embeddedInfinispanLifecycleManager.stopEngine();
	}
	
}
