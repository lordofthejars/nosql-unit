package com.lordofthejars.nosqlunit.cassandra;

import java.io.InputStream;
import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class CassandraOperation extends AbstractCustomizableDatabaseOperation<CassandraConnectionCallback, Keyspace> {

	private Keyspace keyspace;
	private Cluster cluster;
	private CassandraConfiguration cassandraConfiguration;

	protected static final String INTERNAL_CASSANDRA_KEYSPACE = Keyspace.KEYSPACE_SYSTEM;

	public CassandraOperation(CassandraConfiguration cassandraConfiguration) {
		this.cassandraConfiguration = cassandraConfiguration;
		cluster = HFactory.getOrCreateCluster(cassandraConfiguration.getClusterName(), getFullHost());
		setInsertationStrategy(new DefaultCassandraInsertationStrategy());
		setComparisionStrategy(new DefaultCassandraComparisionStrategy());
	}

	@Override
	public void insert(InputStream dataScript) {

		if (this.insertationStrategy instanceof CassandraInsertationStrategy) {
			insertData(dataScript);
		} else {
			throw new IllegalArgumentException(
					"Cassandra insertation strategy must implements CassandraInsertationStrategy interface.");
		}

	}

	private void insertData(InputStream dataScript) {
		try {
			executeInsertation(new CassandraConnectionCallback() {

				@Override
				public Keyspace keyspace() {
					return keyspace;
				}

				@Override
				public Cluster cluster() {
					return cluster;
				}

				@Override
				public CassandraConfiguration cassandraConfiguration() {
					return cassandraConfiguration;
				}
			}, dataScript);
			CassandraInsertationStrategy cassandraInsertationStrategy = (CassandraInsertationStrategy) this.insertationStrategy;
			keyspace = HFactory.createKeyspace(cassandraInsertationStrategy.getKeyspaceName(), cluster);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}

	private String getFullHost() {
		return CassandraHostFormat
				.convert(this.cassandraConfiguration.getHost(), this.cassandraConfiguration.getPort());
	}

	@Override
	public void deleteAll() {
		dropKeyspaces();
	}

	private void dropKeyspaces() {

		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();

		/* drop all keyspace except internal cassandra keyspace */
		for (KeyspaceDefinition keyspaceDefinition : keyspaces) {
			String keyspaceName = keyspaceDefinition.getName();

			if (!INTERNAL_CASSANDRA_KEYSPACE.equals(keyspaceName)) {
				cluster.dropKeyspace(keyspaceName, true);
			}
		}

		List<KeyspaceDefinition> keyspaces2 = cluster.describeKeyspaces();
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		return compareData(expectedData);
	}

	private boolean compareData(InputStream expectedData) throws NoSqlAssertionError {
		try {
			return executeComparision(new CassandraConnectionCallback() {
				
				@Override
				public Keyspace keyspace() {
					return keyspace;
				}
				
				@Override
				public Cluster cluster() {
					return cluster;
				}
				
				@Override
				public CassandraConfiguration cassandraConfiguration() {
					return cassandraConfiguration;
				}
			}, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Keyspace connectionManager() {
		return keyspace;
	}

}
