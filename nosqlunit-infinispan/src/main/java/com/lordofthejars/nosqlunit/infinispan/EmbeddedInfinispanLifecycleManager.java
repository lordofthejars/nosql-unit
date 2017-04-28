package com.lordofthejars.nosqlunit.infinispan;

import java.io.File;
import java.io.IOException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedInfinispanLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedInfinispanLifecycleManager.class);
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = 11222;

	public static final String INMEMORY_INFINISPAN_TARGET_PATH = "target" + File.separatorChar + "infinispan-test-data"
			+ File.separatorChar + "impermanent-db";
	
	private String targetPath = INMEMORY_INFINISPAN_TARGET_PATH;
	
	private String configurationFile = null;
	
	@Override
	public String getHost() {
			return LOCALHOST+targetPath;
	}

	@Override
	public int getPort() {
		return PORT;
	}

	@Override
	public void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Infinispan instance.");
		
		EmbeddedCacheManager embeddedCacheManager = embeddedCacheManager();
		EmbeddedInfinispanInstancesFactory.getInstance().addEmbeddedInstance(embeddedCacheManager, targetPath);
		
		LOGGER.info("Started Embedded Infinispan instance.");
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping Embedded Infinispan instance.");
		EmbeddedInfinispanInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
		LOGGER.info("Stopped Embedded Infinispan instance.");
	}

	private EmbeddedCacheManager embeddedCacheManager() throws IOException {
		if(configurationFile == null) {
			GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
			global.globalJmxStatistics().allowDuplicateDomains(true);
			return new DefaultCacheManager(global.build(), new ConfigurationBuilder().build());
		} else {
			return new DefaultCacheManager(configurationFile);
		}
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public String getTargetPath() {
		return targetPath;
	}
	
	public void setConfigurationFile(String configurationFile) {
		this.configurationFile = configurationFile;
	}
	
}
