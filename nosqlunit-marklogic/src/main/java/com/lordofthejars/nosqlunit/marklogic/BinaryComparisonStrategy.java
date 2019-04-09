package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.content.Content;
import com.lordofthejars.nosqlunit.marklogic.content.DataSetReader;
import com.lordofthejars.nosqlunit.marklogic.content.PassThroughContent;
import com.lordofthejars.nosqlunit.marklogic.content.PassThroughParser;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.lordofthejars.nosqlunit.core.FailureHandler.createFailure;
import static com.marklogic.client.io.InputStreamHandle.newFactory;
import static org.apache.tika.io.IOUtils.contentEquals;
import static org.slf4j.LoggerFactory.getLogger;

class BinaryComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(BinaryComparisonStrategy.class);

    private ContentHandleFactory contentHandleFactory = newFactory();

    private Object target;

    BinaryComparisonStrategy() {
    }

    @Override
    public boolean compare(MarkLogicConnectionCallback connection, InputStream dataSet) throws NoSqlAssertionError {
        PassThroughParser parser = new PassThroughParser(target);
        DataSetReader reader = new DataSetReader(connection.databaseClient().newBinaryDocumentManager(), contentHandleFactory);
        Set<Content> expectedData;
        final Map<String, PassThroughContent> actualData = new HashMap<>();
        try {
            expectedData = parser.parse(dataSet);
            reader.read(expectedData, InputStream.class).forEach((uri, contentHandle) -> actualData.put(uri, new PassThroughContent(uri, contentHandle.get())));
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

    void setTarget(Object target) {
        this.target = target;
    }

    private void compare(Set<Content> expectedSet, Map<String, PassThroughContent> actualSet) {
        for (Content c : expectedSet) {
            PassThroughContent expected = (PassThroughContent) c;
            PassThroughContent actual = actualSet.get(expected.getUri());
            if (actual == null) {
                throw new AssertionError("Expected not available in the actual data set:\n" + expected);
            }
            try (InputStream expectedStream = expected.content();
                 InputStream actualStream = actual.content();) {
                if (!contentEquals(expectedStream, actualStream)) {
                    throw new AssertionError("Expected and actual are not equal:\n" + expected + "\n!=\n" + actual);
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                throw new AssertionError("Exception at\n" + expected + "\n and \n" + actual);
            }
        }
    }
}
