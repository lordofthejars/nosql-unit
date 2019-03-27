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
import org.xmlunit.diff.*;
import org.xmlunit.xpath.JAXPXPathEngine;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import java.io.InputStream;
import java.util.*;

import static com.lordofthejars.nosqlunit.core.FailureHandler.createFailure;
import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_COLLECTIONS;
import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_ID;
import static com.marklogic.client.io.DOMHandle.newFactory;
import static org.slf4j.LoggerFactory.getLogger;
import static org.w3c.dom.Node.TEXT_NODE;
import static org.xmlunit.diff.ComparisonResult.DIFFERENT;
import static org.xmlunit.diff.ComparisonResult.SIMILAR;

class XmlComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(XmlInsertionStrategy.class);

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    private ContentHandleFactory contentHandleFactory = newFactory();

    private Set<String> ignoreProperties = new HashSet<>();

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
        try {
            compare(expectedData, actualData);
        } catch (AssertionError error) {
            throw createFailure(error.getMessage());
        }
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
        this.ignoreProperties = new HashSet(Arrays.asList(ignoreProperties));
    }

    private void compare(Set<Content> expectedSet, Map<String, XmlContent> actualSet) {
        for (Content e : expectedSet) {
            XmlContent expected = (XmlContent) e;
            XmlContent actual = actualSet.get(expected.getUri());
            if (actual == null) {
                throw new AssertionError("Expected not available in the actual data set:\n" + expected);
            }
            Node expectedNode = expected.getData();
            Node actualNode = actual.getData();
            //compares for equality by default
            DiffBuilder diffBuilder = DiffBuilder
                    .compare(expectedNode)
                    .withTest(actualNode)
                    //always ignore control attributes used for document identification
                    .withAttributeFilter(a ->
                            !(ATTR_ID.equalsIgnoreCase(a.getName()) || ATTR_COLLECTIONS.equalsIgnoreCase(a.getName()))
                    )
                    .normalizeWhitespace()
                    .ignoreWhitespace()
                    .ignoreComments();
            //check for similarity if there are properties to ignore
            if (ignoreProperties != null && !ignoreProperties.isEmpty()) {
                DifferenceEvaluator chainedEvaluator = DifferenceEvaluators.chain(
                        DifferenceEvaluators.Default, new IgnoreDifferenceByXPath(ignoreProperties));
                diffBuilder.withDifferenceEvaluator(chainedEvaluator).checkForSimilar();
            }
            Diff diff = diffBuilder.build();
            if (diff.hasDifferences()) {
                throw new AssertionError("Expected and actual are not equal, differences:\n " + diff.toString());
            }
        }
    }

    private static class IgnoreDifferenceByXPath implements DifferenceEvaluator {

        private Set<String> xPathsToIgnore = new HashSet<>();

        private JAXPXPathEngine xpathEngine = new JAXPXPathEngine();

        private IgnoreDifferenceByXPath(final Set<String> xPathsToIgnore) {
            this.xPathsToIgnore = xPathsToIgnore;
        }

        @Override
        public ComparisonResult evaluate(final Comparison comparison, final ComparisonResult outcome) {
            //do some guarding at the beginning
            if (comparison.getControlDetails().getTarget() == null || outcome != DIFFERENT || xPathsToIgnore.isEmpty()) {
                return outcome;
            }
            //go ahead otherwise
            ComparisonResult result = DIFFERENT;
            Comparison.Detail detail = comparison.getTestDetails();
            Node test = detail.getTarget();
            //move one level up for test nodes
            if (TEXT_NODE == test.getNodeType()) {
                test = test.getParentNode();
            }
            final Node searchContext = test.getOwnerDocument().getDocumentElement();
            for (String xpath : xPathsToIgnore) {
                Iterable<Node> i = xpathEngine.selectNodes(xpath, searchContext);
                for (Iterator<Node> it = i.iterator(); it.hasNext(); ) {
                    if (it.next() == test) {
                        LOGGER.debug("{} ({}) ignored by: {}", test.getLocalName(), detail.getXPath(), xpath);
                        result = SIMILAR;
                        break;
                    }
                }
            }
            return result;
        }
    }
}
