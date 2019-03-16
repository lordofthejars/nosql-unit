package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.marklogic.content.DataSetWriter;
import com.lordofthejars.nosqlunit.marklogic.content.PassThrough;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import java.io.InputStream;

import static org.slf4j.LoggerFactory.getLogger;

class TextInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(TextInsertionStrategy.class);

    private ContentHandleFactory contentHandleFactory;

    TextInsertionStrategy(ContentHandleFactory contentHandleFactory) {
        this.contentHandleFactory = contentHandleFactory;
    }

    @Override
    public void insert(MarkLogicConnectionCallback connection, InputStream dataSet) {
        PassThrough parser = new PassThrough();
        DataSetWriter writer = new DataSetWriter(connection.databaseClient().newTextDocumentManager(), contentHandleFactory);
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
