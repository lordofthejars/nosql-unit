package com.lordofthejars.nosqlunit.neo4j;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.neo4j.EmbeddedNeo4j.EmbeddedNeo4jRuleBuilder;

public class SpringEmbeddedNeo4j extends AbstractLifecycleManager {

	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = Configurator.DEFAULT_WEBSERVER_PORT;

	private String targetPath;
	private AbstractGraphDatabase graphDb;

	private BeanFactory beanFactory;
	
	private SpringEmbeddedNeo4j() {
		super();
	}
	
	public static class SpringEmbeddedNeo4jRuleBuilder {

		private SpringEmbeddedNeo4j springEmbeddedNeo4j;

		private SpringEmbeddedNeo4jRuleBuilder() {
			this.springEmbeddedNeo4j = new SpringEmbeddedNeo4j();
		}

		public static SpringEmbeddedNeo4jRuleBuilder newSpringEmbeddedNeo4jRule() {
			return new SpringEmbeddedNeo4jRuleBuilder();
		}

		public SpringEmbeddedNeo4jRuleBuilder beanFactory(BeanFactory beanFactory) {
			this.springEmbeddedNeo4j.setBeanFactory(beanFactory);
			return this;
		}

		public SpringEmbeddedNeo4j build() {
			if (this.springEmbeddedNeo4j.getBeanFactory() == null) {
				throw new IllegalArgumentException("No Bean Factory is provided.");
			}
			return this.springEmbeddedNeo4j;
		}

	}
	
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
		setEmbeddedGraphDatabaseFromBeanFactory();
		setTargetPath();
		
		EmbeddedNeo4jInstances.getInstance().addGraphDatabaseService(graphDb, targetPath);
		registerShutdownHook(graphDb);
	}

	@Override
	protected void doStop() {
		try {
			this.graphDb.shutdown();
			EmbeddedNeo4jInstances.getInstance().removeGraphDatabaseService(targetPath);
		} finally {
			cleanDb();
		}
	}
	
	private void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
	
	private BeanFactory getBeanFactory() {
		return beanFactory;
	}
	
	private void setTargetPath() {
		this.targetPath = uncanonalizeFileFromCurrentWorkingDirectory();
	}

	private String uncanonalizeFileFromCurrentWorkingDirectory() {
		File currentRunDirectory = new File(".");
		String storeDirectory = this.graphDb.getStoreDir();
		String tarPath = storeDirectory.substring(currentRunDirectory.getAbsolutePath().length()-1, storeDirectory.length());
		return tarPath;
	}
	
	private void setEmbeddedGraphDatabaseFromBeanFactory() {
		try {
			this.graphDb = this.beanFactory.getBean(AbstractGraphDatabase.class);
		}catch(NoSuchBeanDefinitionException e) {
			throw new IllegalStateException(e);
		}
	}
	
	private void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
	
	private void cleanDb() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
	}
}
