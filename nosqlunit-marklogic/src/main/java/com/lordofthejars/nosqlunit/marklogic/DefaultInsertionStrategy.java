package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.marklogic.client.io.InputStreamHandle.newFactory;
import static java.util.Optional.ofNullable;
import static org.apache.tika.mime.MediaType.APPLICATION_XML;
import static org.apache.tika.mime.MediaType.OCTET_STREAM;
import static org.apache.tika.mime.MimeTypes.PLAIN_TEXT;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(DefaultInsertionStrategy.class);

    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    private static final TransformerFactory tf = TransformerFactory.newInstance();

    private static ContentHandleFactory contentHandleFactory = newFactory();

    private static TextInsertionStrategy textInsertionStrategy = new TextInsertionStrategy(contentHandleFactory);

    private static XmlInsertionStrategy xmlInsertionStrategy = new XmlInsertionStrategy(contentHandleFactory);

    private static JsonInsertionStrategy jsonInsertionStrategy = new JsonInsertionStrategy(MAPPER, contentHandleFactory);

    private static BinaryInsertionStrategy binaryInsertionStrategy = new BinaryInsertionStrategy(contentHandleFactory);

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

    private static MarkLogicInsertionStrategy insertionStrategy(InputStream dataSet) {
        MarkLogicInsertionStrategy result = null;
        try {
            MediaType mediaType = detectMediaType(dataSet);
            if (APPLICATION_XML.equals(mediaType)) {
                result = xmlInsertionStrategy;
            } else if (APPLICATION_JSON.equals(mediaType)) {
                result = jsonInsertionStrategy;
            } else if (PLAIN_TEXT.equals(mediaType)) {
                result = textInsertionStrategy;
            } else if (OCTET_STREAM.equals(mediaType)) {
                result = binaryInsertionStrategy;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return result;
    }

    @Override
    public void insert(MarkLogicConnectionCallback connection, InputStream dataSet) {
        Optional<MarkLogicInsertionStrategy> strategy = ofNullable(insertionStrategy(dataSet));
        strategy.ifPresent(s -> {
                    try {
                        s.insert(connection, dataSet);
                    } catch (Throwable e) {
                        throw new IllegalArgumentException(e.getMessage(), e);
                    }
                }
        );
    }
}
