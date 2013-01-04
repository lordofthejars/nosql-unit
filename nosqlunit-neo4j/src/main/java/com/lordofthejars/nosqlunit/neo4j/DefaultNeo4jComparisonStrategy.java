package com.lordofthejars.nosqlunit.neo4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.ElementNameAndAttributeQualifier;
import org.custommonkey.xmlunit.XMLUnit;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.graph.parser.GraphMLWriter;

public class DefaultNeo4jComparisonStrategy implements Neo4jComparisonStrategy {

	private static String EOL = System.getProperty("line.separator");
	
	@Override
	public boolean compare(Neo4jConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError, Throwable {

		ByteArrayInputStream neo4jGraphMlRepresentation = getNeo4jContent(connection.graphDatabaseService());
		return compareContents(dataset, neo4jGraphMlRepresentation);
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

	private ByteArrayInputStream getNeo4jContent(GraphDatabaseService graphDatabaseService) {
		Transaction tx = graphDatabaseService.beginTx();

		ByteArrayInputStream neo4jGraphMlRepresentation = null;

		try {
			GraphMLWriter graphMLWriter = new GraphMLWriter(graphDatabaseService);
			neo4jGraphMlRepresentation = readNeo4jData(graphMLWriter);
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

	private ByteArrayInputStream readNeo4jData(GraphMLWriter graphMLWriter) {

		ByteArrayInputStream neo4jGraphMlRepresentation;

		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			graphMLWriter.write(byteArrayOutputStream);
			neo4jGraphMlRepresentation = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
		} catch (XMLStreamException e) {
			throw new IllegalArgumentException(e);
		}

		return neo4jGraphMlRepresentation;
	}
	
}
