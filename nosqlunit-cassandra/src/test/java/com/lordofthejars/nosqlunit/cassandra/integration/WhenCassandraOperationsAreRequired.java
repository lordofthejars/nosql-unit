package com.lordofthejars.nosqlunit.cassandra.integration;

import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandra.EmbeddedCassandraRuleBuilder.newEmbeddedCassandraRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.util.List;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.cassandra.CassandraConfiguration;
import com.lordofthejars.nosqlunit.cassandra.CassandraOperation;
import com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandra;

public class WhenCassandraOperationsAreRequired {

	private static final String INSERT_DATA = "{\n" + 
			"    \"name\" : \"MyKeyspace\",\n" + 
			"    \"columnFamilies\" : [{\n" + 
			"        \"name\" : \"ColumnFamilyName\",\n" + 
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
    public static EmbeddedCassandra cassandraRule = newEmbeddedCassandraRule().build();
	
	@Test
	public void insert_operation_should_add_all_dataset_to_cassandra() {
		
		CassandraOperation cassandraOperation = new CassandraOperation(new CassandraConfiguration("Test Cluster", "localhost", 9171));
		cassandraOperation.insert(new ByteArrayInputStream(INSERT_DATA.getBytes()));
		
		Keyspace keyspaceOperator = (Keyspace) cassandraOperation.connectionManager();
		
		ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
	    columnQuery.setColumnFamily("ColumnFamilyName").setKey("jsmith").setName("first");
	    QueryResult<HColumn<String, String>> result = columnQuery.execute();
	        
	    assertThat(result.get().getValue(), is("John"));
		
	}
	
	@Test
	public void delete_all_operation_should_clean_cassandra() {
		
		CassandraOperation cassandraOperation = new CassandraOperation(new CassandraConfiguration("Test Cluster", "localhost", 9171));
		cassandraOperation.insert(new ByteArrayInputStream(INSERT_DATA.getBytes()));
		
		cassandraOperation.deleteAll();
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", new CassandraHostConfigurator("localhost:9171"));
		/* get all keyspace */
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		
        assertThat(keyspaces, hasSize(3));
		
	}
	
}
