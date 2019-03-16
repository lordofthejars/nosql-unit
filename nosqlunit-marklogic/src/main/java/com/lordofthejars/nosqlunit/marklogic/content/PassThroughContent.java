package com.lordofthejars.nosqlunit.marklogic.content;

import java.io.InputStream;

public class PassThroughContent extends Content<InputStream> {

    protected InputStream data;

    public PassThroughContent(InputStream data) {
        this.data = data;
    }

    public InputStream content() {
        return data;
    }

    @Override
    public InputStream data() {
        return data;
    }
}
