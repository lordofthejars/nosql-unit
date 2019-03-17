package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypes;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;

import static org.slf4j.LoggerFactory.getLogger;

public class MediaTypeDetector {

    private static final Logger LOGGER = getLogger(MediaTypeDetector.class);

    public static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    private final ObjectMapper jsonMapper;

    public MediaTypeDetector(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public MediaType detect(InputStream dataSet) {
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

    private boolean isValidJson(final InputStream is) {
        boolean valid = true;
        is.mark(MimeTypes.getDefaultMimeTypes().getMinLength());
        try {
            jsonMapper.readTree(is);
        } catch (IOException e) {
            valid = false;
        }
        try {
            is.reset();
        } catch (IOException e) {
            LOGGER.trace("couldn't reset data set back to beginning", e);
        }
        return valid;
    }
}
