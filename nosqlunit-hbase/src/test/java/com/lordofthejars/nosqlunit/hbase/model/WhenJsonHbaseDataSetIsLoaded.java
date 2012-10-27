package com.lordofthejars.nosqlunit.hbase.model;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;

public class WhenJsonHbaseDataSetIsLoaded {

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
	public void should_load_json_file_into_dataset() {
		
		DataSetParser dataSetParser = new JsonDataSetParser();
		ParsedDataModel parsedDataset = dataSetParser.parse(new ByteArrayInputStream(HBASE_DATASET.getBytes()));
		
		assertThat(parsedDataset.getName(), is("mytable"));
		
		List<ParsedColumnFamilyModel> columnFamilies = parsedDataset.getColumnFamilies();
		assertThat(columnFamilies, hasSize(1));
		
		ParsedColumnFamilyModel parsedColumnFamilyModel = columnFamilies.get(0);
		assertThat(parsedColumnFamilyModel.getName(), is("mycf"));
		
		List<ParsedRowModel> rows = parsedColumnFamilyModel.getRows();
		assertThat(rows, hasSize(1));
		
		ParsedRowModel parsedRowModel = rows.get(0);
		assertThat(parsedRowModel.getKey(), is("key"));
	
		List<ParsedColumnModel> columns = parsedRowModel.getColumns();
		assertThat(columns, hasSize(2));
		
		ParsedColumnModel firstParsedColumnModel = columns.get(0);
		ParsedColumnModel secondParsedColumnModel = columns.get(1);
		
		assertThat(firstParsedColumnModel.getName(), is("col1"));
		assertThat(firstParsedColumnModel.getValue(), is("val1"));
		
		assertThat(secondParsedColumnModel.getName(), is("col2"));
		assertThat(secondParsedColumnModel.getValue(), is("val2"));
		
	}

}
