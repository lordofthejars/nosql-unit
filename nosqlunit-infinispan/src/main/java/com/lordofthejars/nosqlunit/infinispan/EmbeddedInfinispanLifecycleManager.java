package com.lordofthejars.nosqlunit.infinispan;

import java.io.File;
import java.io.IOException;

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
	protected String getHost() {
			return LOCALHOST+targetPath;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	protected void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Infinispan instance.");
		
		EmbeddedCacheManager embeddedCacheManager = embeddedCacheManager();
		EmbeddedInfinispanInstancesFactory.getInstance().addEmbeddedInstance(embeddedCacheManager, targetPath);
		
		LOGGER.info("Started Embedded InMemory Redis instance.");
	}

	@Override
	protected void doStop() {
		LOGGER.info("Stopping Embedded InMemory Redis instance.");
		EmbeddedInfinispanInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
		LOGGER.info("Stopped Embedded InMemory Redis instance.");
	}

	private EmbeddedCacheManager embeddedCacheManager() throws IOException {
		if(configurationFile == null) {
			return new DefaultCacheManager();
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
