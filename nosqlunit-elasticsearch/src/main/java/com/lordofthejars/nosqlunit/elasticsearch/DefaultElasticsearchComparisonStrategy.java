package com.lordofthejars.nosqlunit.elasticsearch;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.elasticsearch.parser.DataReader;

public class DefaultElasticsearchComparisonStrategy implements ElasticsearchComparisonStrategy {

	@Override
	public boolean compare(ElasticsearchConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
			Throwable {
		Client nodeClient = connection.nodeClient();
		List<Map<String, Object>> documents = DataReader.getDocuments(dataset);
		ElasticsearchAssertion.strictAssertEquals(documents, nodeClient);
		return true;
	}

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }

}
