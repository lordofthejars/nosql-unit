package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.client.Client;

public interface ElasticsearchConnectionCallback {

	Client nodeClient();
	
}
