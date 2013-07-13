package com.lordofthejars.nosqlunit.graph.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.MapUtil;

import com.google.common.base.Strings;

public class Edge {

	private String id;

	private String sourceNodeId;
	private String targetNodeId;

	private String label;

	private Map<String, Object> props = new HashMap<String, Object>();
	private List<ManualIndex> manualIndexes = new ArrayList<Edge.ManualIndex>();

	public Edge(String id, String sourceNodeId, String targetNodeId, String label) {
		this.id = id;
		this.sourceNodeId = sourceNodeId;
		this.targetNodeId = targetNodeId;
		this.label = label;
	}

	public Relationship createLink(Map<String, Node> nodes) {

		Node sourceNode = nodes.get(sourceNodeId);
		Node targetNode = nodes.get(targetNodeId);

		Relationship relationship = sourceNode
				.createRelationshipTo(targetNode, DynamicRelationshipType.withName(label));

		Set<String> keys = props.keySet();

		for (String key : keys) {
			relationship.setProperty(key, props.get(key));
		}

		return relationship;

	}

	public void createManualIndexes(GraphDatabaseService graphDatabaseService, Relationship relationship) {
		for (ManualIndex manualIndex : manualIndexes) {
			if(manualIndex.getConfiguration() == null){
				graphDatabaseService.index().forRelationships(manualIndex.getIndexName())
						.add(relationship, manualIndex.getKey(), manualIndex.getValue());
			} else {
				graphDatabaseService.index().forRelationships(manualIndex.getIndexName(), manualIndex.getConfiguration())
						.add(relationship, manualIndex.getKey(), manualIndex.getValue());
			}
		}
	}

	public void putManualIndex(String indexName, String indexKey, String indexValue) {
		putManualIndex(indexName, indexKey, indexValue, null);
	}

	public void putManualIndex(String indexName, String indexKey, String indexValue, Map<String, String> configuration) {
		this.manualIndexes.add(new ManualIndex(indexName, indexKey, indexValue, configuration));
	}

	public void putData(String key, Object data) {
		this.props.put(key, data);
	}

	public String getLabel() {
		return label;
	}

	public String getId() {
		return id;
	}

	public String getSourceNodeId() {
		return sourceNodeId;
	}

	public String getTargetNodeId() {
		return targetNodeId;
	}

	public Map<String, Object> getProps() {
		return props;
	}

	@Override
	public String toString() {
		return "Edge [id=" + id + ", sourceNodeId=" + sourceNodeId + ", targetNodeId=" + targetNodeId + ", label="
				+ label + "]";
	}

	private class ManualIndex {

		private String indexName;
		private String key;
		private String value;
		private Map<String, String> configuration;

		public ManualIndex(String indexName, String key, String value) {
			super();
			this.indexName = indexName;
			this.key = key;
			this.value = value;
		}

		public ManualIndex(String indexName, String key, String value, Map<String, String> configuration) {
			super();
			this.indexName = indexName;
			this.key = key;
			this.value = value;
			this.configuration = configuration;
		}

		public String getIndexName() {
			return indexName;
		}

		public String getKey() {
			return key;
		}

		public String getValue() {
			return value;
		}

		public Map<String, String> getConfiguration() {
			return configuration;
		}

	}

}
