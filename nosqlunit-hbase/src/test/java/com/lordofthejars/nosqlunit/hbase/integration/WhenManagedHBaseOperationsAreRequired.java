package com.lordofthejars.nosqlunit.hbase.integration;

import static com.lordofthejars.nosqlunit.hbase.ManagedHBase.HBaseRuleBuilder.newManagedHBaseServerRule;
import static com.lordofthejars.nosqlunit.hbase.ManagedHBaseConfigurationBuilder.newManagedHBaseConfiguration;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.hbase.HBaseOperation;
import com.lordofthejars.nosqlunit.hbase.ManagedHBase;

public class WhenManagedHBaseOperationsAreRequired {

	protected static final String LOCALHOST = "127.0.0.1";

	static {
		System.setProperty("JAVA_HOME", "/usr/lib/jvm/java-6-openjdk-i386");
		System.setProperty("HBASE_HOME", "/opt/hbase-0.94.1");
	}
	
	
	@ClassRule
	public static ManagedHBase managedHBase = newManagedHBaseServerRule().build();
	
	private static final String HBASE_DATASET = "{\r\n" + 
			"  \"name\": \"mytable\",\r\n" + 
			"  \"columnFamilies\": [\r\n" + 
			"    {\r\n" + 
			"      \"name\": \"mycf\",\r\n" + 
			"      \"rows\": [\r\n" + 
			"        {\r\n" + 
			"          \"key\": \"key\",\r\n" + 
			"          \"columns\": [\r\n" + 
			"            {\r\n" + 
			"              \"name\": \"col1\",\r\n" + 
			"              \"value\": \"val1\"\r\n" + 
			"            },\r\n" + 
			"            {\r\n" + 
			"              \"name\": \"col2\",\r\n" + 
			"              \"value\": \"val2\"\r\n" + 
			"            }\r\n" + 
			"          ]\r\n" + 
			"        }\r\n" + 
			"      ]\r\n" + 
			"    }\r\n" + 
			"  ]\r\n" + 
			"}";
	
	@Test
	public void insert_operation_should_add_all_dataset_to_hbase() throws IOException {
	
		com.lordofthejars.nosqlunit.hbase.HBaseConfiguration hBaseConfiguration = newManagedHBaseConfiguration().build();
		HBaseOperation hBaseOperation = new HBaseOperation(hBaseConfiguration.getConfiguration());
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		
		HTable table = new HTable(hBaseConfiguration.getConfiguration(), "mytable");
		Get get = new Get("key".getBytes());
		Result result = table.get(get);
		
		assertThat(result.size(), is(2));
		hBaseOperation.deleteAll();
	}
	
	@Test
	public void insert_operation_should_delete_all_dataset_to_hbase() throws IOException {
	
		com.lordofthejars.nosqlunit.hbase.HBaseConfiguration hBaseConfiguration = newManagedHBaseConfiguration().build();
		HBaseOperation hBaseOperation = new HBaseOperation(hBaseConfiguration.getConfiguration());
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		
		hBaseOperation.deleteAll();
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(hBaseConfiguration.getConfiguration());
		
		assertThat(hBaseAdmin.tableExists("mytable"), is(false));
		
	}
}
