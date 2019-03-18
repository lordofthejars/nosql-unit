package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonContent extends Content<JsonNode> {

    private JsonNode data;

    public JsonContent() {
    }

    public JsonContent(JsonNode data) {
        this.data = data;
    }

    public JsonContent(String uri, JsonNode data) {
        this(data);
        setUri(uri);
    }

    @Override
    public InputStream content() {
        return new ByteArrayInputStream(data.toString().getBytes(UTF_8));
    }

    @Override
    public JsonNode getData() {
        return data;
    }

}