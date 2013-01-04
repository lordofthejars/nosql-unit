package com.lordofthejars.nosqlunit.cassandra;

import java.io.InputStream;

import org.cassandraunit.DataLoader;

public class DefaultCassandraInsertionStrategy implements CassandraInsertionStrategy {

	private String keyspaceName;

	@Override
	public void insert(CassandraConnectionCallback connection, InputStream dataset) throws Throwable {
		
		InputStreamJsonDataSet dataSet = new InputStreamJsonDataSet(dataset);
		
		CassandraConfiguration cassandraConfiguration = connection.cassandraConfiguration();
		
		DataLoader dataLoader = new DataLoader(cassandraConfiguration.getClusterName(), getFullHost(cassandraConfiguration));
		dataLoader.load(dataSet);
		
		keyspaceName = dataSet.getKeyspace().getName();
		
	}

	private String getFullHost(CassandraConfiguration cassandraConfiguration) {
		return CassandraHostFormat.convert(cassandraConfiguration.getHost(), cassandraConfiguration.getPort());
	}

	@Override
	public String getKeyspaceName() {
		return keyspaceName;
	}

}
