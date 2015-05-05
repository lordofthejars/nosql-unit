package com.lordofthejars.nosqlunit.graph.parser;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
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
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;

public class WhenImportingGraphMLStream {

    private static final String WELL_FORMED_GRAPH_WITH_ARRAY_VALUES = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
            "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
            "         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
            "        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
            "    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float[]\"/>\n" + 
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
            "            <data key=\"weight\">0.5, 0.5</data>\n" + 
            "        </edge>\n" + 
            "        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
            "            <data key=\"weight\">0.8</data>\n" + 
            "        </edge>\n" + 
            "    </graph>\n" + 
            "</graphml>";
    
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

	private static final String WELL_FORMED_GRAPH_WITH_REFERENCE_NODE = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
			"    <key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"int\"/>\n" + 
			"    <key id=\"lang\" for=\"node\" attr.name=\"lang\" attr.type=\"string\"/>\n" + 
			"    <graph id=\"G\" edgedefault=\"directed\">\n" + 
			"        <node id=\"0\"/>\n" +
			"        <node id=\"15\">\n" +
			"            <data key=\"name\">I</data>\n" +
			"        </node>\n" + 
			"        <node id=\"25\">\n" + 
			"            <data key=\"name\">you</data>\n" + 
			"        </node>\n" + 
			"        <node id=\"3\">\n" + 
			"            <data key=\"name\">him</data>\n" + 
			"        </node>\n" + 
			"        <edge id=\"1\" source=\"0\" target=\"25\" label=\"know\">\n" + 
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
	
	private static final String WELL_FORMED_GRAPH_WITH_NODE_INDEX = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
			"    <key id=\"name\" for=\"node\" attr.autoindexName=\"names\" attr.name=\"name\" attr.type=\"string\"/>\n" + 
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
	
	private static final String WELL_FORMED_GRAPH_WITH_RELATIONSHIP_INDEX = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
			"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" + 
			"         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" + 
			"        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" + 
			"    <key id=\"weight\" for=\"edge\" attr.autoindexName=\"weights\" attr.name=\"weight\" attr.type=\"float\"/>\n" + 
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
	
	private static final String GRAPH_WITH_EDGES_WITH_A_NODE_TYPE_DATA = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
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
			"            <data key=\"name\">0.5</data>\n" + 
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_IN_NODE = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
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
			"			 <index name=\"myindex\" key=\"mykey\">myvalue</index>"+
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
	
	private static final String WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_WITH_CONFIGURATION_IN_NODE = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
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
			"			 <index name=\"myindex\" key=\"mykey\" configuration=\"provider,lucene,type,fulltext\">myvalue</index>"+
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
	
	private static final String WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_IN_EDGES = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
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
			"			 <index name=\"myindex\" key=\"mykey\">myvalue</index>"+
			"        </edge>\n" + 
			"        <edge id=\"2\" source=\"15\" target=\"3\" label=\"know\">\n" + 
			"            <data key=\"weight\">0.8</data>\n" + 
			"        </edge>\n" + 
			"    </graph>\n" + 
			"</graphml>";
	
	private static final String WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_WITH_CONFIGURATION_IN_EDGES = "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" + 
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
			"			 <index name=\"myindex\" key=\"mykey\" configuration=\"type,exact,to_lower_case,true\">myvalue</index>"+
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

	@Test(expected=IllegalArgumentException.class)
	public void parser_should_throw_an_exception_for_data_types_not_decalred_of_the_same_type() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);

		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(GRAPH_WITH_EDGES_WITH_A_NODE_TYPE_DATA.getBytes()));
		
	}
	
	@Test
	public void parser_should_create_manual_indexes_for_nodes() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		Index<Node> index = mock(Index.class);
		
		when(indexManager.forNodes("myindex")).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_IN_NODE.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(1)).add(node15, "mykey", "myvalue");
		
	}
	
	@Test
	public void parser_should_create_manual_indexes_with_configuration_for_nodes() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		Index<Node> index = mock(Index.class);
		
		when(indexManager.forNodes("myindex", MapUtil.stringMap("provider", "lucene", "type", "fulltext"))).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_WITH_CONFIGURATION_IN_NODE.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(1)).add(node15, "mykey", "myvalue");
		
	}
	
	@Test
	public void parser_should_create_manual_indexes_for_relationships() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		RelationshipIndex index = mock(RelationshipIndex.class);
		
		when(indexManager.forRelationships("myindex")).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_IN_EDGES.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(1)).add(relationship1, "mykey", "myvalue");
		
	}
	
	@Test
	public void parser_should_create_manual_indexes_with_configuration_for_relationships() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		RelationshipIndex index = mock(RelationshipIndex.class);
		
		when(indexManager.forRelationships("myindex", MapUtil.stringMap("type", "exact", "to_lower_case", "true"))).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_MANUAL_INDEX_WITH_CONFIGURATION_IN_EDGES.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(1)).add(relationship1, "mykey", "myvalue");
		
	}
	
	@Test
	public void parser_should_create_autoindexes_for_nodes() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		Index<Node> index = mock(Index.class);
		
		when(indexManager.forNodes("names")).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_NODE_INDEX.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(3)).add(any(Node.class), any(String.class), any(Object.class));
		
	}
	
	@Test
	public void parser_should_create_autoindexes_for_relationships() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);
		IndexManager indexManager = mock(IndexManager.class);
		RelationshipIndex index = mock(RelationshipIndex.class);
		
		when(indexManager.forRelationships("weights")).thenReturn(index);
		when(graphDatabaseService.index()).thenReturn(indexManager);
		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_RELATIONSHIP_INDEX.getBytes()));

		verify(graphDatabaseService, times(3)).createNode();

		verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
		verify(index, times(2)).add(any(Relationship.class), any(String.class), any(Object.class));
		
	}
	
	@Test
    public void parser_should_insert_array_data_into_neo4j() throws FileNotFoundException {

        Node node15 = mock(Node.class);
        Node node25 = mock(Node.class);
        Node node3 = mock(Node.class);
        Node referenceNode = mock(Node.class);

        when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
        when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

        Relationship relationship1 = mock(Relationship.class);
        when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

        Relationship relationship2 = mock(Relationship.class);
        when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

        GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);
        graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_ARRAY_VALUES.getBytes()));

        verify(graphDatabaseService, times(3)).createNode();

        verify(node15, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
        verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

        verify(node15).setProperty("name", "I");
        verify(node25).setProperty("name", "you");
        verify(node3).setProperty("name", "him");

        verify(relationship1, times(1)).setProperty("weight", new float[] {0.5f, 0.5f});
        verify(relationship2, times(1)).setProperty("weight", new float[] {0.8f});

    }
	
	@Test
	public void parser_should_insert_data_into_neo4j() throws FileNotFoundException {

		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);

		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
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
	public void parser_should_work_with_misplaced_edges() {
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);

		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
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
		Node referenceNode = mock(Node.class);

		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		
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
	
	@Test
	public void parser_should_use_id_0_as_reference_node() {

		Node referenceNode = mock(Node.class);
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);

		when(graphDatabaseService.createNode()).thenReturn(referenceNode).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(referenceNode.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(WELL_FORMED_GRAPH_WITH_REFERENCE_NODE.getBytes()));

		verify(graphDatabaseService, times(4)).createNode();

		verify(referenceNode, times(1)).createRelationshipTo(eq(node25), any(DynamicRelationshipType.class));
		verify(node15, times(1)).createRelationshipTo(eq(node3), any(DynamicRelationshipType.class));

		verify(node15).setProperty("name", "I");
		verify(node25).setProperty("name", "you");
		verify(node3).setProperty("name", "him");

		verify(relationship1, times(1)).setProperty("weight", (Float) 0.5f);
		verify(relationship2, times(1)).setProperty("weight", (Float) 0.8f);
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_should_throw_an_exception_if_any_edge_is_orphan() {
		
		Node node15 = mock(Node.class);
		Node node25 = mock(Node.class);
		Node node3 = mock(Node.class);
		Node referenceNode = mock(Node.class);

		when(graphDatabaseService.getNodeById(eq(0))).thenReturn(referenceNode);
		
		when(graphDatabaseService.createNode()).thenReturn(node15).thenReturn(node25).thenReturn(node3);

		Relationship relationship1 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node25), any(RelationshipType.class))).thenReturn(relationship1);

		Relationship relationship2 = mock(Relationship.class);
		when(node15.createRelationshipTo(eq(node3), any(RelationshipType.class))).thenReturn(relationship2);

		GraphMLReader graphMLReader = new GraphMLReader(graphDatabaseService);

		graphMLReader.read(new ByteArrayInputStream(ORPHAN_EDGE_GRAPH.getBytes()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void parser_should_throw_an_exception_if_key_is_not_defined() {
		
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
	public void parser_should_throw_an_Exception_if_id_is_defined_in_node_data_section() {
		
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
	public void parser_should_throw_an_exception_if_label_is_defined_in_edge_data_section() {
		
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
