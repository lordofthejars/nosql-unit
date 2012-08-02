package com.lordofthejars.nosqlunit.graph.parser;

import static com.lordofthejars.nosqlunit.graph.parser.TypeCaster.typeCastValue;

import java.io.IOError;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class GraphMLReader {

	private GraphDatabaseService graphDatabaseService;

	public GraphMLReader(GraphDatabaseService graphDatabaseService) {
		this.graphDatabaseService = graphDatabaseService;
	}

	public void read(InputStream stream) {

		XMLInputFactory inputFactory = XMLInputFactory.newInstance();

		try {
			XMLStreamReader reader = inputFactory.createXMLStreamReader(stream);

			Map<String, String> keyIdMap = new HashMap<String, String>();
			Map<String, String> keyTypes = new HashMap<String, String>();

			Map<String, Node> nodes = new HashMap<String, Node>();
			List<Edge> orphanEdges = new ArrayList<Edge>();

			Node currentNode = null;
			Edge currentEdge = null;

			String currentVertexId = null;
			String currentEdgeId = null;

			boolean inVertex = false;
			boolean inEdge = false;
			int graphDepth = 0;

			while (reader.hasNext()) {

				Integer eventType = reader.next();
				if (eventType.equals(XMLEvent.START_ELEMENT)) {

					String elementName = reader.getName().getLocalPart();

					if (elementName.equals(GraphMLTokens.KEY)) {

						String id = reader.getAttributeValue(null, GraphMLTokens.ID);
						String attributeName = reader.getAttributeValue(null, GraphMLTokens.ATTR_NAME);
						String attributeType = reader.getAttributeValue(null, GraphMLTokens.ATTR_TYPE);
						keyIdMap.put(id, attributeName);
						keyTypes.put(attributeName, attributeType);

					} else if (elementName.equals(GraphMLTokens.NODE) && isRootGraph(graphDepth)) {

						currentVertexId = reader.getAttributeValue(null, GraphMLTokens.ID);

						if (currentVertexId != null) {
							Node node = graphDatabaseService.createNode();
							currentNode = node;
							nodes.put(currentVertexId, node);
						}

						inVertex = true;

					} else if (elementName.equals(GraphMLTokens.EDGE) && isRootGraph(graphDepth)) {

						currentEdgeId = reader.getAttributeValue(null, GraphMLTokens.ID);

						String edgeLabel = reader.getAttributeValue(null, GraphMLTokens.LABEL);
						edgeLabel = edgeLabel == null ? GraphMLTokens._DEFAULT : edgeLabel;

						String sourceId = reader.getAttributeValue(null, GraphMLTokens.SOURCE);
						String targetId = reader.getAttributeValue(null, GraphMLTokens.TARGET);

						currentEdge = new Edge(currentEdgeId, sourceId, targetId, edgeLabel);

						inEdge = true;

					} else if (elementName.equals(GraphMLTokens.DATA) && isRootGraph(graphDepth)) {

						String key = reader.getAttributeValue(null, GraphMLTokens.KEY);
						String attributeName = keyIdMap.get(key);

						if (attributeName != null) {
							String value = reader.getElementText();

							if (isInsideNodeTag(inVertex)) {

								if (GraphMLTokens.ID.equals(attributeName)) {
									throw new IllegalArgumentException(
											"id key is reserved for node. Node with errorneous data: "
													+ currentVertexId);
								}

								if (currentNode != null) {
									// inserted directly to neo4j
									currentNode.setProperty(key, typeCastValue(key, value, keyTypes));
								}

							} else {

								if (isInsideEdgeTag(inEdge)) {

									if (GraphMLTokens.LABEL.equals(attributeName)) {
										throw new IllegalArgumentException(
												"label key is reserved for edge. Edge with errorneous data:  "
														+ currentEdgeId);
									}

									if (currentEdge != null) {
										// saved inmemory edge
										currentEdge.putData(key, typeCastValue(key, value, keyTypes));
									}
								}
							}
						} else {
							throw new IllegalArgumentException("Attribute key: " + key + " is not declared.");
						}
						
					} else if (elementName.equals(GraphMLTokens.GRAPH)) {
						nodes.put("0", this.graphDatabaseService.getReferenceNode());
						graphDepth++;
					}

				} else {

					if (eventType.equals(XMLEvent.END_ELEMENT)) {

						String elementName = reader.getName().getLocalPart();

						if (elementName.equals(GraphMLTokens.NODE) && isRootGraph(graphDepth)) {

							currentNode = null;
							currentVertexId = null;
							inVertex = false;

						} else if (elementName.equals(GraphMLTokens.EDGE) && isRootGraph(graphDepth)) {

							addEdge(nodes, orphanEdges, currentEdge);

							currentEdge = null;
							currentEdgeId = null;
							inEdge = false;

						} else if (elementName.equals(GraphMLTokens.GRAPHML)) {
							addOrphanEdgesWithNewParents(nodes, orphanEdges);
						}else if (elementName.equals(GraphMLTokens.GRAPH)) {
							graphDepth--;
						}
					}
				}

			}

			reader.close();

		} catch (XMLStreamException e) {
			throw new IOError(e);
		}

	}

	private boolean isRootGraph(int graphDepth) {
		return graphDepth == 1;
	}
	
	private void addOrphanEdgesWithNewParents(Map<String, Node> nodes, List<Edge> edgesWithoutNodeDefined) {
		for (Edge edge : edgesWithoutNodeDefined) {
			if (isEdgeInsertable(edge, nodes)) {
				edge.createLink(nodes);
			} else {
				throw new IllegalArgumentException("Next edge's nodes has not been declared. " + edge);
			}
		}
	}

	private void addEdge(Map<String, Node> nodes, List<Edge> edgesWithoutNodeDefinedPreviously, Edge currentEdge) {
		if (currentEdge != null && isEdgeInsertable(currentEdge, nodes)) {

			currentEdge.createLink(nodes);

		} else {
			if (currentEdge != null) {
				edgesWithoutNodeDefinedPreviously.add(currentEdge);
			}
		}
	}

	private boolean isEdgeInsertable(Edge edge, Map<String, Node> nodes) {
		return nodes.containsKey(edge.getSourceNodeId()) && nodes.containsKey(edge.getTargetNodeId());
	}

	private boolean isInsideEdgeTag(boolean inEdge) {
		return inEdge == true;
	}

	private boolean isInsideNodeTag(boolean inVertex) {
		return isInsideEdgeTag(inVertex);
	}

}
