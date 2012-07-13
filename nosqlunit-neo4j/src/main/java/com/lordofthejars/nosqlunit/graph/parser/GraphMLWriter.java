package com.lordofthejars.nosqlunit.graph.parser;

import static com.lordofthejars.nosqlunit.graph.parser.TypeCaster.getStringType;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class GraphMLWriter {

	private GraphDatabaseService graphDatabaseService;

	private String xmlSchemaLocation;

	public GraphMLWriter(GraphDatabaseService graphDatabaseService) {
		this.graphDatabaseService = graphDatabaseService;
	}

	public void write(OutputStream os) throws XMLStreamException {

		Map<String, String> vertexKeyTypes = Collections.EMPTY_MAP;
		Map<String, String> edgeKeyTypes = Collections.EMPTY_MAP;

		GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(this.graphDatabaseService);

		List<Node> nodes = loadAllNodes(globalGraphOperations);
		vertexKeyTypes = findNodeKeys(nodes);

		
		List<Relationship> relationships = loadAllRelationships(globalGraphOperations);
		edgeKeyTypes = findRelationshipKeys(relationships);

		final XMLOutputFactory inputFactory = XMLOutputFactory.newInstance();

		XMLStreamWriter writer = inputFactory.createXMLStreamWriter(os, "UTF8");

		writer.writeStartDocument();

		writeRootNode(writer);

		writeKeysForEdges(edgeKeyTypes, writer);
		writeKeysForNodes(vertexKeyTypes, writer);

		writeGraphElement(writer);

		writeNodes(nodes, writer);
		writeRelationships(relationships, writer);

		writeEndGraphs(writer);
		writer.writeEndDocument();

		writer.flush();
		writer.close();

	}
	
	private List<Relationship> loadAllRelationships(GlobalGraphOperations globalGraphOperations) {

		List<Relationship> relationships = new ArrayList<Relationship>();
		
		Iterable<Relationship> relationshipIterable = globalGraphOperations.getAllRelationships();
		Iterator<Relationship> relationshipIterator = relationshipIterable.iterator();

		while (relationshipIterator.hasNext()) {
			relationships.add(relationshipIterator.next());
		}

		return relationships;
		
	}
	

	private List<Node> loadAllNodes(GlobalGraphOperations globalGraphOperations) {

		List<Node> nodes = new ArrayList<Node>();
		
		Iterable<Node> nodeIterable = globalGraphOperations.getAllNodes();
		Iterator<Node> nodeIterator = nodeIterable.iterator();

		while (nodeIterator.hasNext()) {
			nodes.add(nodeIterator.next());
		}

		return nodes;
		
	}

	private Map<String, String> findRelationshipKeys(Iterable<Relationship> relationships) {

		Map<String, String> edgeKeyTypes = new HashMap<String, String>();

		for (Relationship relationship : relationships) {

			Iterable<String> keys = relationship.getPropertyKeys();

			for (String key : keys) {
				if (!edgeKeyTypes.containsKey(key)) {
					edgeKeyTypes.put(key, getStringType(relationship.getProperty(key)));
				}
			}
		}

		return edgeKeyTypes;
	}

	private Map<String, String> findNodeKeys(Iterable<Node> nodes) {
		Map<String, String> vertexKeyTypes = new HashMap<String, String>();

		for (Node node : nodes) {
			Iterable<String> keys = node.getPropertyKeys();

			for (String key : keys) {
				if (!vertexKeyTypes.containsKey(key)) {
					vertexKeyTypes.put(key, getStringType(node.getProperty(key)));
				}
			}
		}
		return vertexKeyTypes;
	}

	private void writeEndGraphs(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeEndElement(); // graph
		writer.writeEndElement(); // graphml
	}

	private void writeRelationships(List<Relationship> relationships, XMLStreamWriter writer) throws XMLStreamException {
		for (Relationship edge : relationships) {
			writer.writeStartElement(GraphMLTokens.EDGE);
			writer.writeAttribute(GraphMLTokens.ID, Long.toString(edge.getId()));
			writer.writeAttribute(GraphMLTokens.SOURCE, Long.toString(edge.getStartNode().getId()));
			writer.writeAttribute(GraphMLTokens.TARGET, Long.toString(edge.getEndNode().getId()));
			writer.writeAttribute(GraphMLTokens.LABEL, edge.getType().name());

			for (String key : edge.getPropertyKeys()) {
				writer.writeStartElement(GraphMLTokens.DATA);
				writer.writeAttribute(GraphMLTokens.KEY, key);
				Object value = edge.getProperty(key);
				if (null != value) {
					writer.writeCharacters(value.toString());
				}
				writer.writeEndElement();
			}
			writer.writeEndElement();
		}
	}

	private void writeNodes(List<Node> nodes, XMLStreamWriter writer) throws XMLStreamException {
		for (Node node : nodes) {
			writer.writeStartElement(GraphMLTokens.NODE);
			writer.writeAttribute(GraphMLTokens.ID, Long.toString(node.getId()));
			Iterable<String> keys = node.getPropertyKeys();

			for (String key : keys) {
				writer.writeStartElement(GraphMLTokens.DATA);
				writer.writeAttribute(GraphMLTokens.KEY, key);
				Object value = node.getProperty(key);
				if (null != value) {
					writer.writeCharacters(value.toString());
				}
				writer.writeEndElement();
			}
			writer.writeEndElement();
		}
	}

	private void writeGraphElement(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeStartElement(GraphMLTokens.GRAPH);
		writer.writeAttribute(GraphMLTokens.ID, GraphMLTokens.G);
		writer.writeAttribute(GraphMLTokens.EDGEDEFAULT, GraphMLTokens.DIRECTED);
	}

	private void writeRootNode(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeStartElement(GraphMLTokens.GRAPHML);
		writer.writeAttribute(GraphMLTokens.XMLNS, GraphMLTokens.GRAPHML_XMLNS);

		// XML Schema instance namespace definition (xsi)
		writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ":" + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
				XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		// XML Schema location
		writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ":"
				+ GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE, GraphMLTokens.GRAPHML_XMLNS
				+ " "
				+ (this.xmlSchemaLocation == null ? GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION
						: this.xmlSchemaLocation));
	}

	private void writeKeysForNodes(Map<String, String> vertexKeyTypes, XMLStreamWriter writer)
			throws XMLStreamException {
		// node keys
		for (String key : vertexKeyTypes.keySet()) {
			writer.writeStartElement(GraphMLTokens.KEY);
			writer.writeAttribute(GraphMLTokens.ID, key);
			writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.NODE);
			writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
			writer.writeAttribute(GraphMLTokens.ATTR_TYPE, vertexKeyTypes.get(key));
			writer.writeEndElement();
		}
	}

	private void writeKeysForEdges(Map<String, String> edgeKeyTypes, XMLStreamWriter writer) throws XMLStreamException {
		// edge key
		for (String key : edgeKeyTypes.keySet()) {
			writer.writeStartElement(GraphMLTokens.KEY);
			writer.writeAttribute(GraphMLTokens.ID, key);
			writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.EDGE);
			writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
			writer.writeAttribute(GraphMLTokens.ATTR_TYPE, edgeKeyTypes.get(key));
			writer.writeEndElement();
		}
	}

}
