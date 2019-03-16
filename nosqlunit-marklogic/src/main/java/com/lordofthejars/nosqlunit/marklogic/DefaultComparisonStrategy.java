package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.marklogic.client.io.InputStreamHandle.newFactory;
import static java.util.Optional.ofNullable;
import static org.apache.tika.mime.MediaType.APPLICATION_XML;
import static org.apache.tika.mime.MediaType.OCTET_STREAM;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(XmlInsertionStrategy.class);

    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private static ContentHandleFactory contentHandleFactory = newFactory();

    private static XmlComparisonStrategy xmlComparisonStrategy = new XmlComparisonStrategy(contentHandleFactory);

    private static JsonComparisonStrategy jsonComparisonStrategy = new JsonComparisonStrategy(MAPPER, contentHandleFactory);

    private static BinaryComparisonStrategy binaryComparisonStrategy = new BinaryComparisonStrategy(contentHandleFactory);

    private static boolean isValidJson(final InputStream is) {
        boolean valid = true;
        try {
            MAPPER.readTree(is);
        } catch (IOException e) {
            valid = false;
        }
        return valid;
    }

    private static MediaType detectMediaType(InputStream dataSet) {
        if (isValidJson(dataSet)) {
            return APPLICATION_JSON;
        }
        try {
            return new DefaultDetector().detect(dataSet, new Metadata());
        } catch (IOException e) {
            LOGGER.warn("Couldn't determine a media type of the data set, cause: " + e.getMessage() + ", returning binary", e);
        }
        return MediaType.OCTET_STREAM;
    }

    private static MarkLogicComparisonStrategy comparisonStrategy(InputStream dataSet) {
        MarkLogicComparisonStrategy result = null;
        try {
            MediaType mediaType = detectMediaType(dataSet);
            if (APPLICATION_XML.equals(mediaType)) {
                result = xmlComparisonStrategy;
            } else if (APPLICATION_JSON.equals(mediaType)) {
                result = jsonComparisonStrategy;
            } else if (OCTET_STREAM.equals(mediaType)) {
                result = binaryComparisonStrategy;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return result;
    }

    @Override
    public boolean compare(MarkLogicConnectionCallback connection, InputStream dataSet) {
        Optional<MarkLogicComparisonStrategy> strategy = ofNullable(comparisonStrategy(dataSet));
        strategy.ifPresent(s -> {
                    try {
                        s.compare(connection, dataSet);
                    } catch (Throwable e) {
                        throw new IllegalArgumentException(e.getMessage(), e);
                    }
                }
        );
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }
}
