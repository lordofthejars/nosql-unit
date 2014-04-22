package com.lordofthejars.nosqlunit.couchbase.model;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentTest.class);

    @Test
    public void testDocumentSerializeAndDeserialize() throws Exception {
        Document expected = new Document("My String", 0);

        String json = mapper.writeValueAsString(expected);
        LOGGER.info("Intermediate json: [{}]", json);
        Document result = mapper.readValue(json, Document.class);
        LOGGER.info("Final object: [{}]", result);
        Assert.assertEquals(expected, result);
    }
}