package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;

import com.lordofthejars.nosqlunit.core.FailureHandler;


public class EmbeddedHBaseConfigurationBuilder {

	private final HBaseConfiguration hBaseConfiguration;
	
	private EmbeddedHBaseConfigurationBuilder() {
		super();
		this.hBaseConfiguration = new HBaseConfiguration();
	}
	
	public static EmbeddedHBaseConfigurationBuilder newEmbeddedHBaseConfiguration() {
		return new EmbeddedHBaseConfigurationBuilder();
	}
	
	public EmbeddedHBaseConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.hBaseConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public HBaseConfiguration build() {
		Configuration defaultConfiguration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		
		if(defaultConfiguration == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedHBase rule during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		this.hBaseConfiguration.setConfiguration(defaultConfiguration);
		return this.hBaseConfiguration;
	}
	
	public HBaseConfiguration buildFromTargetPath(String targetPath) {
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getConfigurationByTargetPath(targetPath);
		
		if(configuration == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedHBase rule with %s target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.", targetPath);
		}
		
		this.hBaseConfiguration.setConfiguration(configuration);
		return this.hBaseConfiguration;
	}
	
}
