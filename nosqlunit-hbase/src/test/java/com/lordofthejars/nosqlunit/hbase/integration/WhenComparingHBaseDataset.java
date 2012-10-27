package com.lordofthejars.nosqlunit.hbase.integration;

import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBase.EmbeddedHBaseRuleBuilder.newEmbeddedHBaseRule;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.hbase.EmbeddedHBase;
import com.lordofthejars.nosqlunit.hbase.EmbeddedHBaseInstances;
import com.lordofthejars.nosqlunit.hbase.HBaseOperation;

public class WhenComparingHBaseDataset {

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
	
	private static final String HBASE_DATASET_DIFFERENT_TABLE = "{\r\n" + 
			"  \"name\": \"mytable2\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_COLUMN_FAMILIES = "{\r\n" + 
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
			"    },\r\n" +
			"	 {\r\n" + 
			"      \"name\": \"mycf2\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_COLUMN_FAMILY_NAME = "{\r\n" + 
			"  \"name\": \"mytable\",\r\n" + 
			"  \"columnFamilies\": [\r\n" + 
			"    {\r\n" + 
			"      \"name\": \"mycf2\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_NUMBER_OF_ROWS = "{\r\n" + 
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
			"        },\r\n" +
			"        {\r\n" + 
			"          \"key\": \"key2\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_KEY_NAME = "{\r\n" + 
			"  \"name\": \"mytable\",\r\n" + 
			"  \"columnFamilies\": [\r\n" + 
			"    {\r\n" + 
			"      \"name\": \"mycf\",\r\n" + 
			"      \"rows\": [\r\n" + 
			"        {\r\n" + 
			"          \"key\": \"key2\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_NUMBER_OF_COLUMNS = "{\r\n" + 
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
			"            }\r\n"+
			"          ]\r\n" + 
			"        }\r\n" + 
			"      ]\r\n" + 
			"    }\r\n" + 
			"  ]\r\n" + 
			"}";
	
	private static final String HBASE_DATASET_DIFFERENT_COLUMN_NAME = "{\r\n" + 
			"  \"name\": \"mytable\",\r\n" + 
			"  \"columnFamilies\": [\r\n" + 
			"    {\r\n" + 
			"      \"name\": \"mycf\",\r\n" + 
			"      \"rows\": [\r\n" + 
			"        {\r\n" + 
			"          \"key\": \"key\",\r\n" + 
			"          \"columns\": [\r\n" + 
			"            {\r\n" + 
			"              \"name\": \"col3\",\r\n" + 
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
	
	private static final String HBASE_DATASET_DIFFERENT_VALUE_NAME = "{\r\n" + 
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
			"              \"value\": \"val2\"\r\n" + 
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
	
	@ClassRule
	public static EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
	
	@After
	public void cleanUp() {
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		hBaseOperation.deleteAll();
	}
	
	@Test
	public void no_exception_should_be_thrown_if_data_is_expected() throws Throwable {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		boolean result = hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		assertThat(result, is(true));
	}
	
	@Test
	public void exception_should_be_thrown_if_table_name_is_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_TABLE.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Table mytable2 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_column_families_are_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_COLUMN_FAMILIES.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of column families are 2 but was 1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_column_family_name_is_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_COLUMN_FAMILY_NAME.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected name of column family is mycf2 but was not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_rows_are_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_NUMBER_OF_ROWS.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of rows are 2 but 1 are found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_key_names_are_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_KEY_NAME.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected row name is key2 but is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_columns_are_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_NUMBER_OF_COLUMNS.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of columns for key are 1 but 2 are found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_column_name_is_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_COLUMN_NAME.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected column are not found. Encountered column with name col1 and value val1 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_value_column_is_different() {
		
		Configuration configuration = EmbeddedHBaseInstances.getInstance().getDefaultConfiguration();
		HBaseOperation hBaseOperation = new HBaseOperation(configuration);
		
		hBaseOperation.insert(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		try {
			hBaseOperation.databaseIs(new ByteArrayInputStream(HBASE_DATASET_DIFFERENT_VALUE_NAME.getBytes()));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected column are not found. Encountered column with name col1 and value val1 is not found."));
		}
	}
	
}
