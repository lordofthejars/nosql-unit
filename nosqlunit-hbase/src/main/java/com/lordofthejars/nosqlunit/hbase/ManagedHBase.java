package com.lordofthejars.nosqlunit.hbase;

import org.junit.rules.ExternalResource;

public class ManagedHBase extends ExternalResource {

	protected ManagedHBaseLifecycleManager managedHBaseLifecycleManager;
	
	
	public static class HBaseRuleBuilder {

		private ManagedHBaseLifecycleManager managedHBaseServerBaseLifecycleManager;

		private HBaseRuleBuilder() {
			this.managedHBaseServerBaseLifecycleManager = new ManagedHBaseLifecycleManager();
		}

		public static HBaseRuleBuilder newManagedHBaseServerRule() {
			return new HBaseRuleBuilder();
		}

		public HBaseRuleBuilder hBasePath(String hBasePath) {
			this.managedHBaseServerBaseLifecycleManager.setHBasePath(hBasePath);
			return this;
		}

		public HBaseRuleBuilder targetPath(String targetPath) {
			this.managedHBaseServerBaseLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public HBaseRuleBuilder port(int port) {
			this.managedHBaseServerBaseLifecycleManager.setPort(port);
			return this;
		}

		public HBaseRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedHBaseServerBaseLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public HBaseRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedHBaseServerBaseLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}
		
		public ManagedHBase build() {
			if (this.managedHBaseServerBaseLifecycleManager.getHBasePath() == null) {
				throw new IllegalArgumentException("No Path to HBase is provided.");
			}
			
			ManagedHBase managedHBase = new ManagedHBase();
			managedHBase.managedHBaseLifecycleManager = this.managedHBaseServerBaseLifecycleManager;
			
			return managedHBase;
		}

	}

	@Override
	public void before() throws Throwable {
		this.managedHBaseLifecycleManager.startEngine();
	}


	@Override
	public void after() {
		this.managedHBaseLifecycleManager.stopEngine();
	}
	
}
