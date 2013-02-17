package com.lordofthejars.nosqlunit.elasticsearch.integration.parser;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Test;

import com.lordofthejars.nosqlunit.elasticsearch.parser.DataReader;

public class WhenElasticsearchDataIsImported {

	private static final String ELASTICSEARCH_DATA = "{\n" + 
			"   \"documents\":[\n" + 
			"      {\n" + 
			"         \"document\":[\n" + 
			"            {\n" + 
			"               \"index\":{\n" + 
			"                  \"indexName\":\"tweeter\",\n" + 
			"                  \"indexType\":\"tweet\",\n" +
			"                  \"indexId\":\"1\"\n" +
			"               }\n" + 
			"            },\n" + 
			"            {\n" + 
			"               \"data\":{\n" + 
			"                  \"name\":\"a\",\n" + 
			"                  \"msg\":\"b\"\n" + 
			"               }\n" + 
			"            }\n" + 
			"         ]\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	
	@Test
	public void data_should_be_indexed() {
		
		Node node = nodeBuilder().local(true).node();
		Client client = node.client();
		
		DataReader dataReader = new DataReader(client);
		dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));
		
		GetResponse response = client.prepareGet("tweeter", "tweet", "1").execute().actionGet();
		Map<String, Object> document = response.sourceAsMap();
		
		assertThat((String)document.get("name"), is("a"));
		assertThat((String)document.get("msg"), is("b"));
		
		node.close();
	}
	
	
}
