package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.marklogic.content.DataSetWriter;
import com.lordofthejars.nosqlunit.marklogic.content.XmlParser;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import java.io.InputStream;

import static org.slf4j.LoggerFactory.getLogger;

class XmlInsertionStrategy implements MarkLogicInsertionStrategy {

    private static final Logger LOGGER = getLogger(XmlInsertionStrategy.class);

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    private ContentHandleFactory contentHandleFactory;

    XmlInsertionStrategy(ContentHandleFactory contentHandleFactory) {
        this.contentHandleFactory = contentHandleFactory;
    }

    @Override
    public void insert(MarkLogicConnectionCallback connection, InputStream dataSet) {
        XmlParser parser = new XmlParser(documentBuilderFactory, transformerFactory);
        DataSetWriter writer = new DataSetWriter(connection.databaseClient().newXMLDocumentManager(), contentHandleFactory);
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
