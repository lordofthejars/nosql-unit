package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.marklogic.content.MediaTypeDetector;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import java.io.InputStream;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET;
import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static com.lordofthejars.nosqlunit.marklogic.content.MediaTypeDetector.APPLICATION_JSON;
import static com.marklogic.client.io.InputStreamHandle.newFactory;
import static java.util.Optional.ofNullable;
import static org.apache.tika.mime.MediaType.APPLICATION_XML;
import static org.apache.tika.mime.MediaType.OCTET_STREAM;
import static org.apache.tika.mime.MimeTypes.PLAIN_TEXT;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(DefaultInsertionStrategy.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(AUTO_CLOSE_TARGET, false)
            .configure(AUTO_CLOSE_SOURCE, false);

    private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    private static final TransformerFactory tf = TransformerFactory.newInstance();

    private static ContentHandleFactory contentHandleFactory = newFactory();

    private static TextInsertionStrategy textInsertionStrategy = new TextInsertionStrategy(contentHandleFactory);

    private static XmlInsertionStrategy xmlInsertionStrategy = new XmlInsertionStrategy(contentHandleFactory);

    private static JsonInsertionStrategy jsonInsertionStrategy = new JsonInsertionStrategy(MAPPER, contentHandleFactory);

    private static BinaryInsertionStrategy binaryInsertionStrategy = new BinaryInsertionStrategy(contentHandleFactory);

    private static MediaTypeDetector mediaTypeDetector = new MediaTypeDetector(MAPPER);

    private static MarkLogicInsertionStrategy insertionStrategy(InputStream dataSet) {
        MarkLogicInsertionStrategy result = null;
        try {
            MediaType mediaType = mediaTypeDetector.detect(dataSet);
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
