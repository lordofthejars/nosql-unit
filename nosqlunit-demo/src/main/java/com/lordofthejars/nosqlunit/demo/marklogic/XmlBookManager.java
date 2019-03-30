package com.lordofthejars.nosqlunit.demo.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JAXBHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;

import javax.xml.bind.JAXBException;

import static com.marklogic.client.io.Format.XML;

public class XmlBookManager extends GenericBookManager {

    private final ContentHandleFactory contentHandleFactory;

    public XmlBookManager(DatabaseClient client) {
        super(client);
        try {
            contentHandleFactory = JAXBHandle.newFactory(Book.class);
        } catch (JAXBException e) {
            throw new IllegalArgumentException("Couldn't instantiate the JAXB factory", e);
        }
    }

    @Override
    protected DocumentManager documentManager() {
        return client.newXMLDocumentManager();
    }

    @Override
    protected Format format() {
        return XML;
    }

    @Override
    protected ContentHandleFactory contentHandleFactory() {
        return contentHandleFactory;
    }
}
