package com.lordofthejars.nosqlunit.elasticsearch;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class LowLevelElasticSearchOperations {


	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;
	
	public boolean assertThatConnectionToElasticsearchIsPossible(String host, int port) throws InterruptedException {
		TransportClient transportClient = new TransportClient();
		
		try {
			transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
			for (int i = 0; i < NUM_RETRIES_TO_CHECK_SERVER_UP; i++) {

				try {
					
					transportClient.admin().cluster().prepareState().execute().actionGet();
					return true;
					
				} catch(Exception e) {
					TimeUnit.SECONDS.sleep(7);
				}				

			}

		} finally {
			transportClient.close();
		}
		
		return false;
	}
	
}
