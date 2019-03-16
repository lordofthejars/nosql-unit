package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonContent extends Content<String> {

    protected String data;

    private JsonNode node;

    public JsonContent(JsonNode data) {
        this.data = data.toString();
    }

    @Override
    public InputStream content() {
        return new ByteArrayInputStream(data.getBytes(UTF_8));
    }

    @Override
    public String data() {
        return data;
    }

    public JsonNode node() {
        return node;
    }
}
