package com.lordofthejars.nosqlunit.hbase;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.IOUtils;

public class EmbeddedHBase extends AbstractLifecycleManager {

	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = HConstants.DEFAULT_MASTER_PORT;
	protected static final String TARGET_DIRECTORY = HBaseTestingUtility.DEFAULT_BASE_TEST_DIRECTORY;
	
	protected static final String DEFAULT_PERMISSIONS = "775";
	
	protected String filePermissions = DEFAULT_PERMISSIONS;
	protected Configuration configuration;
	
	protected EmbeddedHBaseStarter embeddedHBaseStarter;
	
	private EmbeddedHBase() {
		super();
		embeddedHBaseStarter = new EmbeddedHBaseStarter();
	}
	
	public static class EmbeddedHBaseRuleBuilder {

		private EmbeddedHBase embeddedHbase;

		private EmbeddedHBaseRuleBuilder() {
			this.embeddedHbase = new EmbeddedHBase();
		}

		public static EmbeddedHBaseRuleBuilder newEmbeddedHBaseRule() {
			return new EmbeddedHBaseRuleBuilder();
		}

		public EmbeddedHBaseRuleBuilder dirPermissions(String permission) {
			this.embeddedHbase.setFilePermissions(permission);
			return this;
		}

		public EmbeddedHBase build() {
			return this.embeddedHbase;
		}

	}
	
	@Override
	protected String getHost() {
		return LOCALHOST;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	protected void doStart() throws Throwable {
		Configuration config = configuration();
		startMiniCluster(config);
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(this.configuration, LOCALHOST+PORT);
	}

	private void startMiniCluster(Configuration config) throws Exception {
		HBaseTestingUtility testUtil = embeddedHBaseStarter.startSingleMiniCluster(config);
		configuration = testUtil.getConfiguration();
	}

	private Configuration configuration() {
		Configuration config = HBaseConfiguration.create();
		config.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
		config.set("dfs.datanode.data.dir.perm", filePermissions);
		return config;
	}

	@Override
	protected void doStop() {
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration(LOCALHOST+PORT);
		shutdownMiniCluster();
		cleanTargetDirectory();
	}

	private void shutdownMiniCluster() {
		embeddedHBaseStarter.stopMiniCluster();
	}
	
	private void cleanTargetDirectory() {
		File directory = new File(TARGET_DIRECTORY);
		
		if(directory.exists()) {
			IOUtils.deleteDir(directory);
		}
		
	}
	
	public String getFilePermissions() {
		return filePermissions;
	}
	
	public void setFilePermissions(String filePermissions) {
		this.filePermissions = filePermissions;
	}

	protected void setEmbeddedHBaseStarter(EmbeddedHBaseStarter embeddedHBaseStarter) {
		this.embeddedHBaseStarter = embeddedHBaseStarter;
	}
	
}
