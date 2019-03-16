package com.lordofthejars.nosqlunit.marklogic.content;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

public class PassThrough {

    public PassThrough() {
    }

    public Set<Content> parse(InputStream is) throws IOException {
        Set<Content> result = new HashSet<>();
        result.add(new PassThroughContent(is));
        return result;
    }
}
