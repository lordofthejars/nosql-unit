package com.lordofthejars.nosqlunit.couchdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.CouchDbConnector;

public class DataLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String ROOT_ELEMENT = "data";

    private CouchDbConnector connector;

    public DataLoader(CouchDbConnector connector) {
        this.connector = connector;
    }

    public void load(InputStream dataScript) {

        try {
            List<Map<String, Object>> documentsIterator = getDocuments(dataScript);
            insertDocuments(documentsIterator);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(e);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void insertDocuments(List<Map<String, Object>> documentsIterator) {
        for (Map<String, Object> map : documentsIterator) {
            this.connector.create(map);
        }
    }

    public static List<Map<String, Object>> getDocuments(InputStream dataScript)
        throws IOException, JsonProcessingException {
        Map<String, Object> rootNode = MAPPER.readValue(dataScript, Map.class);

        Object dataElements = rootNode.get(ROOT_ELEMENT);

        if (dataElements instanceof List) {
            return (List<Map<String, Object>>) dataElements;
        } else {
            throw new IllegalArgumentException("Array of documents are required.");
        }
    }
}
