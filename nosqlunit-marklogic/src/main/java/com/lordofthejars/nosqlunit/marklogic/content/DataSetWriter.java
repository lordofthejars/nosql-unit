package com.lordofthejars.nosqlunit.marklogic.content;

import com.marklogic.client.Transaction;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class DataSetWriter {

    private DocumentManager documentManager;

    private ContentHandleFactory contentHandleFactory;

    public DataSetWriter(DocumentManager documentManager, ContentHandleFactory contentHandleFactory) {
        this.documentManager = documentManager;
        this.contentHandleFactory = contentHandleFactory;
    }

    public void write(Set<Content> contents, Transaction tx) throws IOException {
        for (Content content : contents) {
            ContentHandle<InputStream> contentHandle = contentHandleFactory.newHandle(InputStream.class);
            DocumentWriteSet writeSet = documentManager.newWriteSet();
            contentHandle.set(content.content());
            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            metadataHandle = metadataHandle.withCollections(content.getCollections());
            writeSet.add(content.getUri(), metadataHandle, contentHandle);
            documentManager.write(writeSet, tx);
        }
    }
}
