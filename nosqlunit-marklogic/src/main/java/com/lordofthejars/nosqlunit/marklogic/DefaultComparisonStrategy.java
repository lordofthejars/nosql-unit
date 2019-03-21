package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.content.MediaTypeDetector;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET;
import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static java.util.Optional.ofNullable;
import static org.apache.tika.mime.MediaType.APPLICATION_XML;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(DefaultComparisonStrategy.class);

    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(AUTO_CLOSE_TARGET, false)
            .configure(AUTO_CLOSE_SOURCE, false);

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private static XmlComparisonStrategy xmlComparisonStrategy = new XmlComparisonStrategy();

    private static JsonComparisonStrategy jsonComparisonStrategy = new JsonComparisonStrategy(MAPPER);

    private static MediaTypeDetector mediaTypeDetector = new MediaTypeDetector(MAPPER);

    private Object target;

    DefaultComparisonStrategy(Object target) {
        this.target = target;
    }

    private static MarkLogicComparisonStrategy comparisonStrategy(InputStream dataSet) {
        MarkLogicComparisonStrategy result = null;
        try {
            MediaType mediaType = mediaTypeDetector.detect(dataSet);
            if (APPLICATION_XML.equals(mediaType)) {
                result = xmlComparisonStrategy;
            } else if (APPLICATION_JSON.equals(mediaType)) {
                result = jsonComparisonStrategy;
            } else {
                result = new BinaryComparisonStrategy();
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
                    prepare(s);
                    try {
                        s.compare(connection, dataSet);
                    } catch (NoSqlAssertionError e) {
                        throw e;
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

    void setTarget(Object target) {
        this.target = target;
    }

    private void prepare(MarkLogicComparisonStrategy strategy) {
        if (strategy instanceof BinaryComparisonStrategy) {
            ((BinaryComparisonStrategy) strategy).setTarget(target);
        }
    }
}
