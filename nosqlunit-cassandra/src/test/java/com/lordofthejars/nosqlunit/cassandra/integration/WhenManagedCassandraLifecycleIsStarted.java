package com.lordofthejars.nosqlunit.cassandra.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandra.ManagedCassandraRuleBuilder.newManagedCassandraRule;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.cassandra.CassandraConfiguration;
import com.lordofthejars.nosqlunit.cassandra.CassandraOperation;
import com.lordofthejars.nosqlunit.cassandra.ManagedCassandra;

public class WhenManagedCassandraLifecycleIsStarted {

	static {
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
	}

	private static final String INSERT_DATA = "{\n" + 
			"    \"name\" : \"MyKeyspace2\",\n" + 
			"    \"columnFamilies\" : [{\n" + 
			"        \"name\" : \"Standard2\",\n" + 
			"		 \"keyType\" : \"UTF8Type\",\n"+
			"        \"defaultColumnValueType\" : \"UTF8Type\",\n"+
			"        \"comparatorType\" : \"UTF8Type\",\n"+
			"        \"rows\" : [{\n" + 
			"            \"key\" : \"jsmith\",\n" + 
			"            \"columns\" : [{\n" + 
			"                \"name\" : \"first\",\n" + 
			"                \"value\" : \"John\"\n" + 
			"            }]\n" + 
			"        }]\n" + 
			"    }]\n" + 
			"}";
	
	@ClassRule
	public static ManagedCassandra managedCassandra = newManagedCassandraRule().build();

	@Test
	public void cassandra_process_should_be_started() {
			
		try {
		CassandraOperation cassandraOperation = new CassandraOperation(new CassandraConfiguration("Test Cluster", "localhost", 9160));
			cassandraOperation.insert(new ByteArrayInputStream(INSERT_DATA.getBytes()));
			cassandraOperation.deleteAll();
		}catch(Throwable t) {
			t.printStackTrace();
			Assert.fail();
		}
	}
	
	
	public void ccassandra_process_should_be_started() throws InterruptedException {

		Cluster createCluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");

		createStructure(createCluster);

		ColumnFamilyTemplate<String, String> columnFamilyTemplate = columnFamilyTemplate(createCluster, "Test Cluster",
				"localhost:9160", "MyKeyspace");

		Mutator<String> mutator = columnFamilyTemplate.createMutator();
		mutator.insert("jsmith", "Standard1", HFactory.createStringColumn("first", "John"));

		ColumnFamilyResult<String, String> res = columnFamilyTemplate.queryColumns("jsmith");
		String value = res.getString("first");

		assertThat(value, is("John"));

		dropStructure(createCluster);
	}

	private void dropStructure(Cluster cluster) {

		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("MyKeyspace");

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keyspaceDef != null) {
				cluster.dropKeyspace("MyKeyspace");
		}
	}

	private void createStructure(Cluster cluster) {

		dropStructure(cluster);
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace", "Standard1",
				ComparatorType.BYTESTYPE);

		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("MyKeyspace");

		if (keyspaceDef == null) {

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace",
					ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));
			cluster.addKeyspace(newKeyspace, true);
		}
	}

	private ColumnFamilyTemplate<String, String> columnFamilyTemplate(Cluster cluster, String clusterName, String host,
			String keyspaceName) {
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

		return new ThriftColumnFamilyTemplate<String, String>(keyspace, "Standard1", StringSerializer.get(),
				StringSerializer.get());

	}

}
