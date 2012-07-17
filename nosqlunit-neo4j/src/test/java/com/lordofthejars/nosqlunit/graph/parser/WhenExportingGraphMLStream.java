package com.lordofthejars.nosqlunit.graph.parser;

import static org.xmlmatchers.XmlMatchers.isEquivalentTo;
import static org.xmlmatchers.transform.XmlConverters.the;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;

import com.lordofthejars.nosqlunit.graph.parser.GraphMLWriter;



public class WhenExportingGraphMLStream {

	private static final String EXPECTED_GRAPH = "<?xml version=\"1.0\" ?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd\"><key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"></key><key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"></key><graph id=\"G\" edgedefault=\"directed\"><node id=\"0\"><data key=\"name\">I</data></node><node id=\"1\"><data key=\"name\">You</data></node><edge id=\"0\" source=\"0\" target=\"1\" label=\"KNOWS\"><data key=\"weight\">0.5</data></edge></graph></graphml>";
	
	@Mock
	GraphDatabaseAPI graphDatabaseAPI;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void writer_Should_Write_Graph_To_GraphML_Format() throws XMLStreamException, UnsupportedEncodingException {
		
		NodeManager nodeManager = mock(NodeManager.class);
		
		Node node1 = mock(Node.class);
		when(node1.getPropertyKeys()).thenReturn(Arrays.asList("name"));
		when(node1.getProperty("name")).thenReturn("I");
		when(node1.getId()).thenReturn(0L);
		
		Node node2 = mock(Node.class);
		when(node2.getPropertyKeys()).thenReturn(Arrays.asList("name"));
		when(node2.getProperty("name")).thenReturn("You");
		when(node2.getId()).thenReturn(1L);
		
		List<Node> nodes = new ArrayList<Node>();
		nodes.add(node1);
		nodes.add(node2);
		when(nodeManager.getAllNodes()).thenReturn(nodes.iterator());

		Relationship relationship1 = mock(Relationship.class);
		when(relationship1.getPropertyKeys()).thenReturn(Arrays.asList("weight"));
		when(relationship1.getProperty("weight")).thenReturn(0.5f);
		when(relationship1.getId()).thenReturn(0L);
		when(relationship1.getStartNode()).thenReturn(node1);
		when(relationship1.getEndNode()).thenReturn(node2);
		RelationshipType relationshipType = mock(RelationshipType.class);
		when(relationshipType.name()).thenReturn("KNOWS");
		when(relationship1.getType()).thenReturn(relationshipType);

		List<Relationship> relationships = new ArrayList<Relationship>();
		relationships.add(relationship1);
		
		when(nodeManager.getAllRelationships()).thenReturn(relationships.iterator());
		
		when(graphDatabaseAPI.getNodeManager()).thenReturn(nodeManager);
		

		GraphMLWriter graphMLWriter = new GraphMLWriter(graphDatabaseAPI);
		
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		graphMLWriter.write(output);
		
		String generatedGraph = new String(output.toByteArray(), "UTF-8");
		assertThat(the(generatedGraph), isEquivalentTo(the(EXPECTED_GRAPH)));
		
	}
	
}
