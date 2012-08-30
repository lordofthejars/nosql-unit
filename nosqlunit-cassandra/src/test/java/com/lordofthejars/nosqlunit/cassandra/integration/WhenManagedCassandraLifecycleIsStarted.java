package com.lordofthejars.nosqlunit.cassandra.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandra.ManagedCassandraRuleBuilder.newManagedCassandraRule;

import java.util.Arrays;

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

import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.cassandra.ManagedCassandra;

public class WhenManagedCassandraLifecycleIsStarted {

	static {
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
	}
	
	
	@ClassRule
	public static ManagedCassandra managedCassandra = newManagedCassandraRule().build();
	
	@Test
	public void cassandra_process_should_be_started() {
		
		createStructure();
		
		ColumnFamilyTemplate<String, String> columnFamilyTemplate = columnFamilyTemplate("Test Cluster", "localhost:9160", "MyKeyspace");
		
		Mutator<String> mutator = columnFamilyTemplate.createMutator();
		mutator.insert("jsmith", "Standard1", HFactory.createStringColumn("first", "John"));
		
		ColumnFamilyResult<String, String> res = columnFamilyTemplate.queryColumns("jsmith");
	    String value = res.getString("first");
		
	    assertThat(value, is("John"));
	    
		dropStructure();
	}

	private void dropStructure() {
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","localhost:9160");
		
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("MyKeyspace");
		
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef != null) {
		    cluster.dropKeyspace("MyKeyspace");
		}
	}
	
	private void createStructure() {
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","localhost:9160");
		dropStructure();
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace",                              
                "Standard1", 
                ComparatorType.BYTESTYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace",                 
                ThriftKsDef.DEF_STRATEGY_CLASS,  
                1, 
                Arrays.asList(cfDef));
		cluster.addKeyspace(newKeyspace, true);
	}
	
	private ColumnFamilyTemplate<String, String> columnFamilyTemplate(String clusterName, String host, String keyspaceName) {
		Cluster cluster = HFactory.getOrCreateCluster(clusterName, host);
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		
        return  new ThriftColumnFamilyTemplate<String, String>(keyspace,
        		"Standard1", 
                                                               StringSerializer.get(),        
                                                               StringSerializer.get());
        
	}
	
}
