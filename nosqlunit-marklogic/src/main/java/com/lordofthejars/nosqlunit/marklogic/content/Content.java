package com.lordofthejars.nosqlunit.marklogic.content;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_COLLECTION;
import static java.util.Arrays.asList;

public abstract class Content<D> {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private String uri;

    private Set<String> collections = new HashSet<>(asList(DEFAULT_COLLECTION));

    protected Content() {
    }

    protected Content(Set<String> collections) {
        this.collections.addAll(collections);
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

    public void addCollection(String collection) {
        collections.add(collection);
    }

    public abstract InputStream content() throws IOException;

    public abstract D getData();

    @Override
    public String toString() {
        return "Content{" +
                "uri='" + uri + '\'' +
                ", collections=" + collections +
                '}';
    }
}
