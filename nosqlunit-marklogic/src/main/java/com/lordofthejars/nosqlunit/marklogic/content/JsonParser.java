package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

public class JsonParser {

    private ObjectMapper jsonMapper;

    public JsonParser(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public Set<Content> parse(InputStream is) throws IOException {
        Set<Content> result = new HashSet<>();
        ObjectReader reader = jsonMapper.readerFor(JsonContent.class);
        MappingIterator<JsonContent> iter = reader.readValues(is);
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        return result;
    }

    public JsonNode node(JsonContent content) {
        if (content.node() != null) {
            return content.node();
        }
        try (Reader reader = new InputStreamReader(content.content())) {
            return jsonMapper.readValue(reader, JsonNode.class);
        } catch (IOException e) {
            getLogger(getClass()).error(e.getMessage(), e);
        }
        return null;
    }
}
