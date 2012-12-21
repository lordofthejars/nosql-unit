package com.lordofthejars.nosqlunit.neo4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.ElementNameAndAttributeQualifier;
import org.custommonkey.xmlunit.XMLUnit;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.graph.parser.GraphMLReader;
import com.lordofthejars.nosqlunit.graph.parser.GraphMLWriter;

public class Neo4jOperation implements DatabaseOperation<GraphDatabaseService> {

	private static String EOL = System.getProperty("line.separator");

	private GraphDatabaseService graphDatabaseService;

	private GraphMLReader graphMLReader;
	private GraphMLWriter graphMLWriter;

	public Neo4jOperation(GraphDatabaseService graphDatabaseService) {
		super();
		this.graphDatabaseService = graphDatabaseService;
		this.graphMLReader = new GraphMLReader(this.graphDatabaseService);
		this.graphMLWriter = new GraphMLWriter(this.graphDatabaseService);
	}

	@Override
	public void insert(InputStream dataScript) {

		Transaction tx = this.graphDatabaseService.beginTx();

		try {
			this.graphMLReader.read(dataScript);
			tx.success();
		} finally {
			tx.finish();
		}

	}

	@Override
	public void deleteAll() {
		Transaction tx = this.graphDatabaseService.beginTx();

		try {
			
			Iterator<Node> allNodes = Neo4jLowLevelOps.getAllNodes(graphDatabaseService);
			Iterator<Relationship> allRelationships = Neo4jLowLevelOps.getAllRelationships(graphDatabaseService);
			
			removeAllRelationships(allRelationships);
			removeAllNodes(allNodes);

			tx.success();
		} finally {
			tx.finish();
		}
	}

	private void removeAllNodes(Iterator<Node> allNodes) {
		
		while(allNodes.hasNext()) {
			Node node = allNodes.next();
			if (isNotReferenceNode(node)) {
				node.delete();
			}
		}
	}

	private boolean isNotReferenceNode(Node node) {
		return node.getId() != 0;
	}

	private void removeAllRelationships(Iterator<Relationship> allRelationships) {
		while(allRelationships.hasNext()) {
			allRelationships.next().delete();
		}
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {

		ByteArrayInputStream neo4jGraphMlRepresentation = getNeo4jContent();
		return compareContents(expectedData, neo4jGraphMlRepresentation);

	}

	private boolean compareContents(InputStream expectedData, ByteArrayInputStream neo4jGraphMlRepresentation) {
		try {
			configureXmlUnit();

			Diff diff = new Diff(new InputSource(neo4jGraphMlRepresentation), new InputSource(expectedData));
			diff.overrideElementQualifier(new ElementNameAndAttributeQualifier());

			if (diff.similar()) {
				return true;
			} else {
				String differenceMessage = buildDifferenceMessage(diff);
				throw FailureHandler.createFailure(differenceMessage);
			}

		} catch (SAXException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private ByteArrayInputStream getNeo4jContent() {
		Transaction tx = this.graphDatabaseService.beginTx();

		ByteArrayInputStream neo4jGraphMlRepresentation = null;

		try {
			neo4jGraphMlRepresentation = readNeo4jData();
			tx.success();
		} finally {
			tx.finish();
		}
		return neo4jGraphMlRepresentation;
	}

	private String buildDifferenceMessage(Diff diff) {
		DetailedDiff detailedDiff = new DetailedDiff(diff);
		@SuppressWarnings("unchecked")
		List<Difference> differences = detailedDiff.getAllDifferences();
		StringBuilder message = new StringBuilder(
				"Some differences has been found between database data and expected data:");
		message.append(EOL);

		for (Difference difference : differences) {
			message.append("************************");
			message.append(difference);
			message.append("************************");
		}

		String differenceMessage = message.toString();
		return differenceMessage;
	}

	private void configureXmlUnit() {
		XMLUnit.setIgnoreComments(true);
		XMLUnit.setIgnoreWhitespace(true);
		XMLUnit.setNormalizeWhitespace(true);
		XMLUnit.setIgnoreDiffBetweenTextAndCDATA(true);
		XMLUnit.setCompareUnmatched(false);
		XMLUnit.setIgnoreAttributeOrder(true);
	}

	private ByteArrayInputStream readNeo4jData() {

		ByteArrayInputStream neo4jGraphMlRepresentation;

		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			this.graphMLWriter.write(byteArrayOutputStream);
			neo4jGraphMlRepresentation = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
		} catch (XMLStreamException e) {
			throw new IllegalArgumentException(e);
		}

		return neo4jGraphMlRepresentation;
	}


	@Override
	public GraphDatabaseService connectionManager() {
		return this.graphDatabaseService;
	}

	public void setGraphMLReader(GraphMLReader graphMLReader) {
		this.graphMLReader = graphMLReader;
	}

	public void setGraphMLWriter(GraphMLWriter graphMLWriter) {
		this.graphMLWriter = graphMLWriter;
	}

}
