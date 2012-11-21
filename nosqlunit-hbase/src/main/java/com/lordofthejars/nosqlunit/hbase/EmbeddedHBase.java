package com.lordofthejars.nosqlunit.hbase;

import org.junit.rules.ExternalResource;

public class EmbeddedHBase extends ExternalResource {

	private EmbeddedHBase() {
		super();
	}
	
	protected EmbeddedHBaseLifecycleManager embeddedHBaseLifecycleManager;
	
	public static class EmbeddedHBaseRuleBuilder {

		private EmbeddedHBaseLifecycleManager embeddedHBaseLifecycleManager;

		private EmbeddedHBaseRuleBuilder() {
			this.embeddedHBaseLifecycleManager = new EmbeddedHBaseLifecycleManager();
		}

		public static EmbeddedHBaseRuleBuilder newEmbeddedHBaseRule() {
			return new EmbeddedHBaseRuleBuilder();
		}

		public EmbeddedHBaseRuleBuilder dirPermissions(String permission) {
			this.embeddedHBaseLifecycleManager.setFilePermissions(permission);
			return this;
		}

		public EmbeddedHBase build() {
			
			EmbeddedHBase embeddedHBase = new EmbeddedHBase();
			embeddedHBase.embeddedHBaseLifecycleManager = this.embeddedHBaseLifecycleManager;
			return embeddedHBase;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.embeddedHBaseLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.embeddedHBaseLifecycleManager.stopEngine();
	}
	
	
}
