package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

/**
 * The wrapper class for JSON data sets.
 */
public class JsonContents {

    private Map<String, JsonContent> contents = new HashMap<>();

    @JsonAnyGetter
    public Map<String, JsonContent> getContents() {
        return contents;
    }

    @JsonAnySetter
    public void addContent(String uri, JsonContent content) {
        contents.put(uri, content);
    }
}
