package com.lordofthejars.nosqlunit.marklogic.content;

import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public class DataSetReader {

    private DocumentManager documentManager;

    private ContentHandleFactory contentHandleFactory;

    public DataSetReader(DocumentManager documentManager, ContentHandleFactory contentHandleFactory) {
        this.documentManager = documentManager;
        this.contentHandleFactory = contentHandleFactory;
    }

    public <C> Map<String, ContentHandle<C>> read(Set<Content> contents, Class<C> type) {
        return contents.stream().collect(toMap(Content::getUri, c ->
                        (ContentHandle<C>) documentManager.read(c.getUri(), contentHandleFactory.newHandle(type))
                )
        );
    }
}
