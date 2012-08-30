package com.lordofthejars.nosqlunit.cassandra;

import java.io.InputStream;
import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.DataLoader;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CassandraOperation implements DatabaseOperation<Keyspace> {

	private Keyspace keyspace;
	private Cluster cluster;
	private CassandraConfiguration cassandraConfiguration;
	
	protected static final String INTERNAL_CASSANDRA_KEYSPACE = Keyspace.KEYSPACE_SYSTEM;
	
	public CassandraOperation(CassandraConfiguration cassandraConfiguration) {
		this.cassandraConfiguration = cassandraConfiguration;
		cluster = HFactory.getOrCreateCluster(cassandraConfiguration.getClusterName(), getFullHost());
	}
	
	@Override
	public void insert(InputStream dataScript) {
		
		InputStreamJsonDataSet dataSet = new InputStreamJsonDataSet(dataScript);
		
		DataLoader dataLoader = new DataLoader(cassandraConfiguration.getClusterName(), getFullHost());
		dataLoader.load(dataSet);
		
		keyspace = HFactory.createKeyspace(dataSet.getKeyspace().getName(), cluster);
	}

	private String getFullHost() {
		return CassandraHostFormat.convert(this.cassandraConfiguration.getHost(), this.cassandraConfiguration.getPort());
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
				cluster.dropKeyspace(keyspaceName);
			}
		}
		
		List<KeyspaceDefinition> keyspaces2 = cluster.describeKeyspaces();
	}
	
	@Override
	public boolean databaseIs(InputStream expectedData) {
		CassandraAssertion.strictAssertEquals(new InputStreamJsonDataSet(expectedData), cluster, keyspace);
		return true;
	}

	@Override
	public Keyspace connectionManager() {
		return keyspace;
	}

}
