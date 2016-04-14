package com.lordofthejars.nosqlunit.elasticsearch2;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class LowLevelElasticSearchOperations {
	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

	public boolean assertThatConnectionToElasticsearchIsPossible(String host, int port) throws InterruptedException {
		final InetSocketAddress address = new InetSocketAddress(host, port);

		try (TransportClient transportClient = TransportClient.builder().build()) {
			transportClient.addTransportAddress(new InetSocketTransportAddress(address));
			for (int i = 0; i < NUM_RETRIES_TO_CHECK_SERVER_UP; i++) {
				try {
					transportClient.admin().cluster().prepareState().execute().actionGet();
					return true;
				} catch (Exception e) {
					TimeUnit.SECONDS.sleep(7);
				}
			}
		}

		return false;
	}
}
