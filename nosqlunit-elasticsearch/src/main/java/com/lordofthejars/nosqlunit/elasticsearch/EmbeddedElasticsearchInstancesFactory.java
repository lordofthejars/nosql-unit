package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.node.Node;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;

public class EmbeddedElasticsearchInstancesFactory {

	private static EmbeddedInstances<Node> embeddedInstances;
	
	private EmbeddedElasticsearchInstancesFactory() {
		super();
	}

	public synchronized static EmbeddedInstances<Node> getInstance() {
		if(embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<Node>();
		}
		
		return embeddedInstances;
	}
	
}
