package com.lordofthejars.nosqlunit.neo4j;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import com.lordofthejars.nosqlunit.graph.parser.GraphMLReader;

public class WhenImportingGraphMLStream {

	private static final String WELL_FORMED_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";

	private static final String MISPLACED_EDGES_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String ORPHAN_EDGE_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node>\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String NOT_DEFINED_KEY_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String NODE_WITH_ID_ATTRIBUTE_DATA_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <key id=\"id\" for=\"node\" attr.name=\"id\" attr.type=\"string\"/>\n" +
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"id\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String EDGE_WITH_LABEL_ATTRIBUTE_DATA_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>\n" +
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"label\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String SUBGRAPH_DATA_GRAPH = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"15\">\n" + 
			"            <data key=\"name\">I</data>\n" + 
			"            <graph id=\"H\" edgedefault=\"directed\">\n" + 
			"            	<node id=\"20\">\n" + 
			"            		<data key=\"name\">Her</data>\n" + 
			"            	</node>\n" + 
			"            </graph>\n" + 
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"15\" target=\"25\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	
	
	@Mock
	private GraphDatabaseService graphDatabaseService;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void parser_Should_Insert_Data_Into_Neo4j() throws FileNotFoundException {

		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);

	}

	@Test
	public void parser_Should_Work_With_Misplaced_Edges() {
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(MISPLACED_EDGES_GRAPH.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
	}
	
	@Test
	public void parser_should_ignore_all_subgraphs() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(SUBGRAPH_DATA_GRAPH.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_Should_Throw_An_Exception_If_Any_Edge_Is_Orphan() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(ORPHAN_EDGE_GRAPH.getBytes()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_Should_Throw_An_Exception_If_Key_Is_Not_Defined() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(NOT_DEFINED_KEY_GRAPH.getBytes()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_Should_Throw_An_Exception_If_Id_Is_Defined_In_Node_Data_Section() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(NODE_WITH_ID_ATTRIBUTE_DATA_GRAPH.getBytes()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_Should_Throw_An_Exception_If_Label_Is_Defined_In_Edge_Data_Section() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(EDGE_WITH_LABEL_ATTRIBUTE_DATA_GRAPH.getBytes()));
	}
	
}
