package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.marklogic.content.DataSetWriter;
import com.lordofthejars.nosqlunit.marklogic.content.JsonParser;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import java.io.InputStream;

import static org.slf4j.LoggerFactory.getLogger;

public class JsonInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(JsonInsertionStrategy.class);

    private ObjectMapper mapper;

    private ContentHandleFactory contentHandleFactory;

    JsonInsertionStrategy(ObjectMapper mapper, ContentHandleFactory contentHandleFactory) {
        this.mapper = mapper;
        this.contentHandleFactory = contentHandleFactory;
    }

    @Override
    public void insert(MarkLogicConnectionCallback connection, InputStream dataSet) {
        JsonParser parser = new JsonParser(mapper);
        DataSetWriter writer = new DataSetWriter(connection.databaseClient().newJSONDocumentManager(), contentHandleFactory);
        Transaction tx = connection.databaseClient().openTransaction();
        try {
            writer.write(parser.parse(dataSet), tx);
            tx.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            tx.rollback();
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}