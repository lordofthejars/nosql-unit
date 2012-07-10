package com.lordofthejars.nosqlunit.graph.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Edge {

	private String id;
	
	private String sourceNodeId;
	private String targetNodeId;
	
	private String label;
	
	private Map<String, Object> props = new HashMap<String, Object>();
	
	public Edge(String id, String sourceNodeId, String targetNodeId, String label) {
		this.id = id;
		this.sourceNodeId = sourceNodeId;
		this.targetNodeId = targetNodeId;
		this.label = label;
	}
	
	public void createLink(Map<String, Node> nodes) {
		
		Node sourceNode = nodes.get(sourceNodeId);
		Node targetNode = nodes.get(targetNodeId);
		
		Relationship relationship = sourceNode.createRelationshipTo(targetNode,  DynamicRelationshipType.withName(label));
		
		Set<String> keys = props.keySet();
		
		for (String key : keys) {
			relationship.setProperty(key, props.get(key));
		}
		
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
		return "Edge [id=" + id + ", sourceNodeId=" + sourceNodeId
				+ ", targetNodeId=" + targetNodeId + ", label=" + label + "]";
	}
	
}
