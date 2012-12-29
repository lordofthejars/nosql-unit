package com.lordofthejars.nosqlunit.infinispan;

import org.junit.rules.ExternalResource;

public class ManagedInfinispan extends ExternalResource {

	protected ManagedInfinispanLifecycleManager managedInfinispanLifecycleManager;
	
	private ManagedInfinispan() {
		super();
	}
	
	public static class ManagedInfinispanRuleBuilder {
		
		private ManagedInfinispanLifecycleManager managedInfinispanLifecycleManager;
		
		private ManagedInfinispanRuleBuilder() {
			this.managedInfinispanLifecycleManager = new ManagedInfinispanLifecycleManager();
		}
		
		public static ManagedInfinispanRuleBuilder newManagedInfinispanRule() {
			return new ManagedInfinispanRuleBuilder();
		}
		
		public ManagedInfinispanRuleBuilder port(int port) {
			this.managedInfinispanLifecycleManager.setPort(port);
			return this;
		}
		
		public ManagedInfinispanRuleBuilder targetPath(String targetPath) {
			this.managedInfinispanLifecycleManager.setTargetPath(targetPath);
			return this;
		}
		
		public ManagedInfinispanRuleBuilder infinispanPath(String infinispanPath) {
			this.managedInfinispanLifecycleManager.setInfinispanPath(infinispanPath);
			return this;
		}
		
		public ManagedInfinispanRuleBuilder protocol(String protocol) {
			this.managedInfinispanLifecycleManager.setProtocol(protocol);
			return this;
		}
		
		public ManagedInfinispanRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedInfinispanLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public ManagedInfinispanRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedInfinispanLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}
		
		public ManagedInfinispan build() {
			
			if(this.managedInfinispanLifecycleManager.getProtocol() == null) {
				throw new IllegalArgumentException("At least not null protocol should be provided [memcached|hotrod|websocket].");
			}

			ManagedInfinispan managedInfinispan = new ManagedInfinispan();
			managedInfinispan.managedInfinispanLifecycleManager = this.managedInfinispanLifecycleManager;
			
			
			return managedInfinispan;
			
		}
		
	}
	
	@Override
	protected void before() throws Throwable {
		this.managedInfinispanLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.managedInfinispanLifecycleManager.stopEngine();
	}

}
