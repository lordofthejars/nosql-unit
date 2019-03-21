package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.marklogic.content.DataSetWriter;
import com.lordofthejars.nosqlunit.marklogic.content.PassThroughParser;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import java.io.InputStream;

import static org.slf4j.LoggerFactory.getLogger;

class BinaryInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(BinaryInsertionStrategy.class);

    private ContentHandleFactory contentHandleFactory;

    private Object target;

    BinaryInsertionStrategy(ContentHandleFactory contentHandleFactory) {
        this.contentHandleFactory = contentHandleFactory;
    }

    @Override
    public void insert(MarkLogicConnectionCallback connection, InputStream dataSet) {
        PassThroughParser parser = new PassThroughParser(target);
        DataSetWriter writer = new DataSetWriter(connection.databaseClient().newBinaryDocumentManager(), contentHandleFactory);
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

    void setTarget(Object target) {
        this.target = target;
    }
}
