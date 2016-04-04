package com.lordofthejars.nosqlunit.elasticsearch2.integration.parser;

import com.lordofthejars.nosqlunit.elasticsearch2.parser.DataReader;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenElasticsearchDataIsImported {
	private static final String ELASTICSEARCH_DATA = "{\n" +
			"   \"documents\":[\n" +
			"	  {\n" +
			"		 \"document\":[\n" +
			"			{\n" +
			"			   \"index\":{\n" +
			"				  \"indexName\":\"tweeter\",\n" +
			"				  \"indexType\":\"tweet\",\n" +
			"				  \"indexId\":\"1\"\n" +
			"			   }\n" +
			"			},\n" +
			"			{\n" +
			"			   \"data\":{\n" +
			"				  \"name\":\"a\",\n" +
			"				  \"msg\":\"b\"\n" +
			"			   }\n" +
			"			}\n" +
			"		 ]\n" +
			"	  }\n" +
			"   ]\n" +
			"}";

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void data_should_be_indexed() throws IOException {
		final String pathHome = temporaryFolder.newFolder().getAbsolutePath();
		final Settings settings = nodeBuilder().settings().put("path.home", pathHome).build();

		try (final Node node = nodeBuilder().local(true).settings(settings).node();
			 final Client client = node.client()) {
			final DataReader dataReader = new DataReader(client);
			dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

			final GetResponse response = client.prepareGet("tweeter", "tweet", "1").execute().actionGet();
			final Map<String, Object> document = response.getSourceAsMap();

			assertThat((String) document.get("name"), is("a"));
			assertThat((String) document.get("msg"), is("b"));
		}
	}
}
