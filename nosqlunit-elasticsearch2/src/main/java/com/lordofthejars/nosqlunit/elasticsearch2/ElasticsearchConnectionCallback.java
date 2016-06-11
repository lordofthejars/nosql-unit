package com.lordofthejars.nosqlunit.elasticsearch2;

import org.elasticsearch.client.Client;

public interface ElasticsearchConnectionCallback {
	Client nodeClient();
}
