package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

public class RemoteHBaseConfigurationBuilder {

	private final HBaseConfiguration hBaseConfiguration;

	private RemoteHBaseConfigurationBuilder() {
		super();
		this.hBaseConfiguration = new HBaseConfiguration();

		Configuration configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
		configuration.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
		this.hBaseConfiguration.setConfiguration(configuration);
	}

	public static RemoteHBaseConfigurationBuilder newRemoteHBaseConfiguration() {
		return new RemoteHBaseConfigurationBuilder();
	}

	public RemoteHBaseConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.hBaseConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}

	public RemoteHBaseConfigurationBuilder setProperty(String name, String value) {
		this.hBaseConfiguration.getConfiguration().set(name, value);
		return this;
	}

	public HBaseConfiguration build() {
		return this.hBaseConfiguration;
	}

}
