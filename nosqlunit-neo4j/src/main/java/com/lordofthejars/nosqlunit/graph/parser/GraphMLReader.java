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

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.MapUtil;

import com.google.common.base.Strings;

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
			Map<String, String> keyTypesForNodes = new HashMap<String, String>();
			Map<String, String> keyTypesForEdges = new HashMap<String, String>();
			Map<String, String> keyAutoindexesForNodes = new HashMap<String, String>();
			Map<String, String> keyAutoindexesForEdges = new HashMap<String, String>();

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
						String attributeAutoindexName = reader.getAttributeValue(null, GraphMLTokens.ATTR_AUTOINDEX);

						String attributeFor = reader.getAttributeValue(null, GraphMLTokens.FOR);

						keyIdMap.put(id, attributeName);

						if (GraphMLTokens.NODE.equalsIgnoreCase(attributeFor)) {

							keyTypesForNodes.put(attributeName, attributeType);

							if (attributeAutoindexName != null) {
								keyAutoindexesForNodes.put(attributeName, attributeAutoindexName);
							}

						} else {
							if (GraphMLTokens.EDGE.equalsIgnoreCase(attributeFor)) {

								keyTypesForEdges.put(attributeName, attributeType);

								if (attributeAutoindexName != null) {
									keyAutoindexesForEdges.put(attributeName, attributeAutoindexName);
								}
							}
						}

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

						String attributeName = reader.getAttributeValue(null, GraphMLTokens.KEY);

						if (isInsideNodeTag(inVertex)) {

							if (keyTypesForNodes.containsKey(attributeName)) {

								String value = reader.getElementText();
								Object typeCastValue = typeCastValue(attributeName, value, keyTypesForNodes);

								if (GraphMLTokens.ID.equals(attributeName)) {
									throw new IllegalArgumentException(
											"id key is reserved for node. Node with errorneous data: "
													+ currentVertexId);
								}

								if (currentNode != null) {
									// inserted directly to neo4j
									currentNode.setProperty(attributeName, typeCastValue);

									if (keyAutoindexesForNodes.containsKey(attributeName)) {
										String autoindexName = keyAutoindexesForNodes.get(attributeName);
										this.graphDatabaseService.index().forNodes(autoindexName)
												.add(currentNode, attributeName, typeCastValue);
									}

								}
							} else {
								throw new IllegalArgumentException("Attribute key: " + attributeName
										+ " is not declared.");
							}

						} else {

							if (isInsideEdgeTag(inEdge)) {

								if (keyTypesForEdges.containsKey(attributeName)) {

									String value = reader.getElementText();
									Object typeCastValue = typeCastValue(attributeName, value, keyTypesForEdges);

									if (GraphMLTokens.LABEL.equals(attributeName)) {
										throw new IllegalArgumentException(
												"label key is reserved for edge. Edge with errorneous data:  "
														+ currentEdgeId);
									}

									if (currentEdge != null) {
										// saved inmemory edge
										currentEdge.putData(attributeName, typeCastValue);
									}
								} else {
									throw new IllegalArgumentException("Attribute key: " + attributeName
											+ " is not declared.");
								}
							}
						}

					} else if (elementName.equals(GraphMLTokens.INDEX) && isRootGraph(graphDepth)) {
						
						if (isInsideNodeTag(inVertex)) {
							//add custom index over currentNode
							String indexName = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_NAME);
							String indexKey = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_KEY);
							String indexConfiguration = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_CONFIGURATION);
							String indexData = reader.getElementText();
							
							if(Strings.isNullOrEmpty(indexConfiguration)) {
								this.graphDatabaseService.index().forNodes(indexName).add(currentNode, indexKey, indexData);
							} else {
								String[] indexConfigurationTokens = indexConfiguration.split(GraphMLTokens.ATTR_INDEX_CONFIGURATION_SEPARATOR);
								this.graphDatabaseService.index().forNodes(indexName, MapUtil.stringMap(indexConfigurationTokens)).add(currentNode, indexKey, indexData);
							}
							
						} else {
							if (isInsideEdgeTag(inEdge)) {
								//add custom index over currentEdge
								String indexName = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_NAME);
								String indexKey = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_KEY);
								String indexConfiguration = reader.getAttributeValue(null, GraphMLTokens.ATTR_INDEX_CONFIGURATION);
								String indexData = reader.getElementText();
								
								if(Strings.isNullOrEmpty(indexConfiguration)) {
									currentEdge.putManualIndex(indexName, indexKey, indexData);
								} else {
									String[] indexConfigurationTokens = indexConfiguration.split(GraphMLTokens.ATTR_INDEX_CONFIGURATION_SEPARATOR);
									currentEdge.putManualIndex(indexName, indexKey, indexData, MapUtil.stringMap(indexConfigurationTokens));
								}
							}
						}
						
						
					} else if(elementName.equals(GraphMLTokens.GRAPH)) {
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

							addEdge(nodes, orphanEdges, currentEdge, keyAutoindexesForEdges);

							currentEdge = null;
							currentEdgeId = null;
							inEdge = false;

						} else if (elementName.equals(GraphMLTokens.GRAPHML)) {
							addOrphanEdgesWithNewParents(nodes, orphanEdges, keyAutoindexesForEdges);
						} else if (elementName.equals(GraphMLTokens.GRAPH)) {
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

	private void addOrphanEdgesWithNewParents(Map<String, Node> nodes, List<Edge> edgesWithoutNodeDefined,
			Map<String, String> indexes) {
		for (Edge edge : edgesWithoutNodeDefined) {
			if (isEdgeInsertable(edge, nodes)) {
				Relationship relationship = edge.createLink(nodes);
				
				edge.createManualIndexes(graphDatabaseService, relationship);
				
				createRelationshipAutoIndexes(relationship, edge.getProps(), indexes);
			} else {
				throw new IllegalArgumentException("Next edge's nodes has not been declared. " + edge);
			}
		}
	}

	private void addEdge(Map<String, Node> nodes, List<Edge> edgesWithoutNodeDefinedPreviously, Edge currentEdge,
			Map<String, String> indexes) {
		if (currentEdge != null && isEdgeInsertable(currentEdge, nodes)) {

			Relationship relationship = currentEdge.createLink(nodes);
			currentEdge.createManualIndexes(graphDatabaseService, relationship);
			
			createRelationshipAutoIndexes(relationship, currentEdge.getProps(), indexes);

		} else {
			if (currentEdge != null) {
				edgesWithoutNodeDefinedPreviously.add(currentEdge);
			}
		}
	}

	private void createRelationshipAutoIndexes(Relationship relationship, Map<String, Object> props,
			Map<String, String> indexes) {

		for (String prop : props.keySet()) {
			if (indexes.containsKey(prop)) {
				String indexName = indexes.get(prop);
				this.graphDatabaseService.index().forRelationships(indexName).add(relationship, prop, props.get(prop));
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
