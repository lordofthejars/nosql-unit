package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.content.Content;
import com.lordofthejars.nosqlunit.marklogic.content.DataSetReader;
import com.lordofthejars.nosqlunit.marklogic.content.XmlContent;
import com.lordofthejars.nosqlunit.marklogic.content.XmlParser;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.Diff;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import java.io.InputStream;
import java.util.*;

import static com.lordofthejars.nosqlunit.core.FailureHandler.createFailure;
import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_COLLECTIONS;
import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_ID;
import static com.marklogic.client.io.DOMHandle.newFactory;
import static org.slf4j.LoggerFactory.getLogger;

class XmlComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(XmlInsertionStrategy.class);

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    private ContentHandleFactory contentHandleFactory = newFactory();

    private Set<String> ignoreProperties;

    XmlComparisonStrategy() {
    }

    @Override
    public boolean compare(MarkLogicConnectionCallback connection, InputStream dataSet) throws NoSqlAssertionError {
        XmlParser parser = new XmlParser(documentBuilderFactory, transformerFactory);
        DataSetReader reader = new DataSetReader(connection.databaseClient().newXMLDocumentManager(), contentHandleFactory);
        Set<Content> expectedData;
        final Map<String, XmlContent> actualData = new HashMap<>();
        try {
            expectedData = parser.parse(dataSet);
            reader.read(expectedData, Document.class).forEach((uri, contentHandle) -> actualData.put(uri, new XmlContent(contentHandle.get())));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new NoSqlAssertionError(e.getMessage());
        }
        if (expectedData.size() != actualData.size()) {
            throw createFailure("Expected number of documents is: %s but actual number was: %s", expectedData.size(), actualData.size());
        }
        if (!compare(expectedData, actualData)) {
            throw createFailure("Expected documents and actual document don't match exactly, see log warnings for details!");
        }
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
        this.ignoreProperties = new HashSet(Arrays.asList(ignoreProperties));
    }

    private boolean compare(Set<Content> expectedSet, Map<String, XmlContent> actualSet) {
        boolean result = true;
        for (Content e : expectedSet) {
            XmlContent expected = (XmlContent) e;
            XmlContent actual = actualSet.get(expected.getUri());
            if (actual == null) {
                result = false;
                LOGGER.warn("Expected not available in the actual data set:\n{}", expected);
                continue;
            }
            Node expectedNode = expected.getData();
            Node actualNode = actual.getData();
            Diff diff = DiffBuilder
                    .compare(expectedNode)
                    .withTest(actualNode)
                    .withAttributeFilter(a ->
                            !(ATTR_ID.equalsIgnoreCase(a.getName()) || ATTR_COLLECTIONS.equalsIgnoreCase(a.getName()))
                    )
                    //.withNodeFilter(n -> !ignoreProperties.contains(n.getLocalName()))
                    .normalizeWhitespace()
                    .ignoreWhitespace()
                    .ignoreComments()
                    .build();
            if (diff.hasDifferences()) {
                result = false;
                LOGGER.warn("Expected and actual are not equal, differences:\n{}", diff.toString());
            }
        }
        return result;
    }
}
