package com.lordofthejars.nosqlunit.marklogic.content;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;
import java.util.stream.Collectors;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_COLLECTION;
import static org.slf4j.LoggerFactory.getLogger;

public class JsonParser {

    private ObjectMapper jsonMapper;

    public JsonParser(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public Set<Content> parse(InputStream is) throws IOException {
        ObjectReader reader = jsonMapper.readerFor(JsonContents.class);
        JsonContents contents = reader.readValue(is);
        contents.getContents().entrySet().forEach(e -> {
            e.getValue().setUri(e.getKey());
            e.getValue().addCollection(DEFAULT_COLLECTION);
        });
        return contents.getContents().values().stream().collect(Collectors.toSet());
    }

    public JsonNode node(JsonContent content) {
        if (content.getData() != null) {
            return content.getData();
        }
        try (Reader reader = new InputStreamReader(content.content())) {
            return jsonMapper.readValue(reader, JsonNode.class);
        } catch (IOException e) {
            getLogger(getClass()).error(e.getMessage(), e);
        }
        return null;
    }
}
