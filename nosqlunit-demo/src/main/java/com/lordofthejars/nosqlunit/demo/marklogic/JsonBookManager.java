package com.lordofthejars.nosqlunit.demo.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonDatabindHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;

import static com.marklogic.client.io.Format.JSON;

public class JsonBookManager extends GenericBookManager {

    private final ContentHandleFactory contentHandleFactory;

    private DatabaseClient client;

    public JsonBookManager(DatabaseClient client) {
        super(client);
        contentHandleFactory = JacksonDatabindHandle.newFactory(Book.class);
    }

    @Override
    protected DocumentManager documentManager() {
        return client.newJSONDocumentManager();
    }

    @Override
    protected Format format() {
        return JSON;
    }

    @Override
    protected ContentHandleFactory contentHandleFactory() {
        return contentHandleFactory;
    }
}
