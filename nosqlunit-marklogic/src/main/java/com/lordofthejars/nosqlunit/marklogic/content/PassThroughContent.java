package com.lordofthejars.nosqlunit.marklogic.content;

import java.io.InputStream;

public class PassThroughContent extends Content<InputStream> {

    protected InputStream data;

    public PassThroughContent(InputStream data) {
        this.data = data;
    }

    public PassThroughContent(String uri, InputStream data) {
        this(data);
        setUri(uri);
    }

    public InputStream content() {
        return getData();
    }

    @Override
    public InputStream getData() {
        return data;
    }
}
