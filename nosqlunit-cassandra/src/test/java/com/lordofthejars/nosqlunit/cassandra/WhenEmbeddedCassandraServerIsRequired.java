package com.lordofthejars.nosqlunit.cassandra;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class WhenEmbeddedCassandraServerIsRequired {

	@Test
	public void embedded_cassandra_should_be_able_to_receive_data() throws TTransportException, IOException,
			InterruptedException, ConfigurationException {

		EmbeddedCassandraServerHelper embeddedCassandraServerHelper = new EmbeddedCassandraServerHelper();
		embeddedCassandraServerHelper.startEmbeddedCassandra(
				"cu-cassandra.yaml","target" + File.separatorChar + "cassandra-temp");

		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9171");
		
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace", "ColumnFamilyName", ComparatorType.BYTESTYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS,  1, Arrays.asList(cfDef));

		cluster.addKeyspace(newKeyspace, true);
		
		Keyspace keyspaceOperator = HFactory.createKeyspace("MyKeyspace", cluster);
		
		Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
        mutator.insert("jsmith", "ColumnFamilyName", HFactory.createStringColumn("first", "John"));
        
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
        columnQuery.setColumnFamily("ColumnFamilyName").setKey("jsmith").setName("first");
        QueryResult<HColumn<String, String>> result = columnQuery.execute();
        
        assertThat(result.get().getValue(), is("John"));
		
        cluster.getConnectionManager().shutdown();

		embeddedCassandraServerHelper.stopEmbeddedCassandra();

	}

}
