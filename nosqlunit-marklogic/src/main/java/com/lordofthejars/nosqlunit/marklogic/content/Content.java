package com.lordofthejars.nosqlunit.marklogic.content;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_COLLECTION;

public abstract class Content<D> {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private String uri;

    private Set<String> collections = new HashSet<>();

    protected Content() {
        collections.add(DEFAULT_COLLECTION);
    }

    protected Content(Set<String> collections) {
        collections.addAll(collections);
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String[] getCollections() {
        return collections.toArray(EMPTY_STRING_ARRAY);
    }

    public abstract InputStream content() throws IOException;

    public abstract D data();

    @Override
    public String toString() {
        return "Content{" +
                "uri='" + uri + '\'' +
                ", collections=" + collections +
                '}';
    }
}
