package com.lordofthejars.nosqlunit.elasticsearch.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearch.EmbeddedElasticsearchRuleBuilder.newEmbeddedElasticsearchRule;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.elasticsearch.ElasticsearchOperation;
import com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearch;
import com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearchInstancesFactory;

public class WhemEmbeddedElasticsearchOperationsAreRequired {

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
	
	
	@ClassRule
	public static EmbeddedElasticsearch embeddedElasticsearch = newEmbeddedElasticsearchRule().build();
	
	@After
	public void removeIndexes() {
		Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(node.client());
		deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		deleteByQueryRequestBuilder.execute().actionGet();
		
		node.client().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	
	@Test
	public void insert_operation_should_index_all_dataset() {
		
		Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(node.client());
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));
		
		GetResponse document = node.client().prepareGet("tweeter", "tweet", "1").execute().actionGet();
		Map<String, Object> documentSource = document.getSource();
		
		//Strange a cast to Object
		assertThat(documentSource, hasEntry("name", (Object)"a"));
		assertThat(documentSource, hasEntry("msg", (Object)"b"));
	}
	
	@Test
	public void delete_operation_should_delete_all_indexes() {
		
		Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(node.client());
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));
		
		elasticsearchOperation.deleteAll();
		
		GetResponse document = node.client().prepareGet("tweeter", "tweet", "1").execute().actionGet();
		assertThat(document.exists(), is(false));
		
	}
	
	@Test
	public void database_is_operation_should_compare_database() {
		
		Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(node.client());
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));
		
		boolean databaseIs = elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));
		assertThat(databaseIs, is(databaseIs));
		
	}
	
}
