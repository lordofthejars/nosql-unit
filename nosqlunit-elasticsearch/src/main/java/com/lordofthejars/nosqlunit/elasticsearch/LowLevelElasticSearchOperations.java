package com.lordofthejars.nosqlunit.elasticsearch;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class LowLevelElasticSearchOperations {


	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;
	
	public boolean assertThatConnectionToElasticsearchIsPossible(String host, int port) throws InterruptedException {
		TransportClient transportClient = new TransportClient();
		
		try {
			transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
			for (int i = 0; i < NUM_RETRIES_TO_CHECK_SERVER_UP; i++) {

				ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();

				if (!connectedNodes.isEmpty()) {
					return true;
				}

				TimeUnit.SECONDS.sleep(3);
			}

		} finally {
			transportClient.close();
		}
		
		return false;
	}
	
}
