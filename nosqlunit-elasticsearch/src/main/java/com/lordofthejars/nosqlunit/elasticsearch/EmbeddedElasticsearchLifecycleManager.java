package com.lordofthejars.nosqlunit.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedElasticsearchLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedElasticsearchLifecycleManager.class); 
	private static final String LOCALHOST = "127.0.0.1";
	private static final int DEFAULT_PORT = 9300;
	private static final String DATA_PATH_PROPERTY = "path.data";
	
	
	public static final String EMBEDDED_ELASTICSEARCH_TARGET_PATH = "target" + File.separatorChar + "elasticsearch-test-data"
			+ File.separatorChar + "impermanent-db";
	
	private String targetPath = EMBEDDED_ELASTICSEARCH_TARGET_PATH;
	
	private NodeBuilder nodeBuilder;
	
	public EmbeddedElasticsearchLifecycleManager() {
		nodeBuilder = nodeBuilder().local(true);
	}
	
	@Override
	public String getHost() {
		return LOCALHOST+targetPath;
	}

	@Override
	public int getPort() {
		return DEFAULT_PORT;
	}
	
	@Override
	public void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Elasticsearch instance.");
		
		nodeBuilder.getSettings().put(DATA_PATH_PROPERTY, targetPath);
		Node node = elasticsearchNode();
		EmbeddedElasticsearchInstancesFactory.getInstance().addEmbeddedInstance(node, targetPath);
		
		LOGGER.info("Started Embedded Elasticsearch instance.");
	}

	private Node elasticsearchNode() {
		return nodeBuilder.node();
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping Embedded Elasticsearch instance.");
		
		Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getEmbeddedByTargetPath(targetPath);
		
		if(node != null) {
			node.close();
		}
		
		EmbeddedElasticsearchInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
		LOGGER.info("Stopped Embedded Elasticsearch instance.");
		
	}
	
	public void setSettings(Settings settings) {
		nodeBuilder.settings(settings);
	}
	
	public void setLoadConfigSettings(boolean loadConfigSettings) {
		nodeBuilder.loadConfigSettings(loadConfigSettings);
	}
	
	public void setClient(boolean client) {
		nodeBuilder.client(client);
	}
	
	public void setClusterName(String clusterName) {
		nodeBuilder.clusterName(clusterName);
	}
	
	public void setData(boolean data) {
		nodeBuilder.data(data);
	}
	
	public void setLocal(boolean local) {
		nodeBuilder.local(local);
	}
	
	public String getTargetPath() {
		return targetPath;
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

}
