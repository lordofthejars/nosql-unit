package com.lordofthejars.nosqlunit.elasticsearch2;

import com.lordofthejars.nosqlunit.elasticsearch2.parser.DataReader;

import java.io.InputStream;

public class DefaultElasticsearchInsertionStrategy implements ElasticsearchInsertionStrategy {
	@Override
	public void insert(ElasticsearchConnectionCallback connection, InputStream dataset) throws Throwable {
		DataReader dataReader = new DataReader(connection.nodeClient());
		dataReader.read(dataset);
	}
}
