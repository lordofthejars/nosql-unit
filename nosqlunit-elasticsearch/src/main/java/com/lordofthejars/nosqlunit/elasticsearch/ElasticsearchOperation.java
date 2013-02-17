package com.lordofthejars.nosqlunit.elasticsearch;

import java.io.InputStream;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class ElasticsearchOperation extends AbstractCustomizableDatabaseOperation<ElasticsearchConnectionCallback, Client> {

	private Client client;
	
	public ElasticsearchOperation(Client client) {
		this.client = client;
		setInsertionStrategy(new DefaultElasticsearchInsertionStrategy());
		setComparisonStrategy(new DefaultElasticsearchComparisonStrategy());
	}
	
	@Override
	public void insert(InputStream dataScript) {
		insertData(dataScript);
	}

	private void insertData(InputStream dataScript) {
		try {
			executeInsertion(new ElasticsearchConnectionCallback() {
				
				@Override
				public Client nodeClient() {
					return client;
				}
			}, dataScript);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	@Override
	public void deleteAll() {
		clearDocuments();
	}

	private void clearDocuments() {
		
		DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
		deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		deleteByQueryRequestBuilder.execute().actionGet();
		
		refreshNode();
		
	}
	
	private void refreshNode() {
		client.admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@Override
	public boolean databaseIs(InputStream expectedData) {
		try {
			return executeComparison(new ElasticsearchConnectionCallback() {
				
				@Override
				public Client nodeClient() {
					return client;
				}
			}, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Client connectionManager() {
		return client;
	}

}
