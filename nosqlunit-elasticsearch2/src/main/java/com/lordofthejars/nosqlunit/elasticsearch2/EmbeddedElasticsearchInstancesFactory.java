package com.lordofthejars.nosqlunit.elasticsearch2;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;
import org.elasticsearch.node.Node;

public class EmbeddedElasticsearchInstancesFactory {
	private static EmbeddedInstances<Node> embeddedInstances;

	private EmbeddedElasticsearchInstancesFactory() {
	}

	public synchronized static EmbeddedInstances<Node> getInstance() {
		if (embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<>();
		}

		return embeddedInstances;
	}

}
