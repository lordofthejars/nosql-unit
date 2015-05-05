package com.lordofthejars.nosqlunit.graph.parser;

import static org.junit.Assert.assertThat;
import static org.xmlmatchers.XmlMatchers.isEquivalentTo;
import static org.xmlmatchers.transform.XmlConverters.the;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;


public class WhenExportingGraphMLStream {

	private static final String EXPECTED_GRAPH = "<?xml version=\"1.0\" ?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\"><key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"></key><key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"></key><graph id=\"G\" edgedefault=\"directed\"><node id=\"0\"><data key=\"name\">I</data></node><node id=\"1\"><data key=\"name\">You</data></node><edge id=\"0\" source=\"0\" target=\"1\" label=\"KNOWS\"><data key=\"weight\">0.5</data></edge></graph></graphml>";
	private static final String EXPECTED_GRAPH_WITH_ARRAY ="<?xml version=\"1.0\" ?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\"><key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float[]\"></key><key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"></key><graph id=\"G\" edgedefault=\"directed\"><node id=\"0\"><data key=\"name\">I</data></node><node id=\"1\"><data key=\"name\">You</data></node><edge id=\"0\" source=\"0\" target=\"1\" label=\"KNOWS\"><data key=\"weight\">0.5, 0.5</data></edge></graph></graphml>";
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void writer_Should_Write_Graph_To_GraphML_Format() throws XMLStreamException, UnsupportedEncodingException {

		String generatedGraph = renderGraphToGraphML("I", "You", 0.5f);
		assertThat(the(generatedGraph), isEquivalentTo(the(EXPECTED_GRAPH)));
		
	}

	@Test
	public void writer_Should_Write_arrays_values_Graph_To_GraphML_Format() throws XMLStreamException, UnsupportedEncodingException {
		String generatedGraph = renderGraphToGraphML("I", "You", new float[]{0.5f, 0.5f});
		System.out.println(generatedGraph);
		assertThat(the(generatedGraph), isEquivalentTo(the(EXPECTED_GRAPH_WITH_ARRAY)));
	}

	private String renderGraphToGraphML(String name1, String name2, Object weight) throws XMLStreamException, UnsupportedEncodingException {
		String generatedGraph;GraphDatabaseService graphDatabase = new TestGraphDatabaseFactory().newImpermanentDatabase();
		Transaction tx = graphDatabase.beginTx();
		try {
			Node node1 = graphDatabase.createNode();
			node1.setProperty("name", name1);

			Node node2 = graphDatabase.createNode();
			node2.setProperty("name", name2);

			Relationship relationship1 = node1.createRelationshipTo(node2, DynamicRelationshipType.withName("KNOWS"));
			relationship1.setProperty("weight", weight);

			GraphMLWriter graphMLWriter = new GraphMLWriter(graphDatabase);

			ByteArrayOutputStream output = new ByteArrayOutputStream();
			graphMLWriter.write(output);

			generatedGraph = new String(output.toByteArray(), "UTF-8");

			tx.success();
		} finally {
			tx.close();
			graphDatabase.shutdown();
		}
		return generatedGraph;
	}

}
