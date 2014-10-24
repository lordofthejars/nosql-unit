package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.JsonParseException;
import com.lordofthejars.nosqlunit.couchbase.model.Document;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.MapType;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@AllArgsConstructor
public class DataLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String DATA_ROOT = "data";

    public static final String DESIGN_ROOT = "designDocs";// TODO

    private CouchbaseClient couchbaseClient;

    public void load(final InputStream dataScript) {
        final Map<String, Document> documentsIterator = getDocuments(dataScript);
        insertDocuments(documentsIterator);
    }

    @SneakyThrows({ExecutionException.class, InterruptedException.class, IOException.class})
    private void insertDocuments(final Map<String, Document> documentsIterator) {
        for (final Map.Entry<String, Document> documentEntry : documentsIterator.entrySet()) {
            final Document document = documentEntry.getValue();
            couchbaseClient.add(documentEntry.getKey(), document.calculateExpiration(),
                    MAPPER.writeValueAsString(document.getDocument())).get();
        }
    }

    @SneakyThrows({JsonParseException.class, JsonMappingException.class, IOException.class})
    public static Map<String, Document> getDocuments(final InputStream dataScript) {
        TypeFactory typeFactory = MAPPER.getTypeFactory();
        final MapType mapType = typeFactory.constructMapType(Map.class, String.class, Document.class);
        JavaType stringType = typeFactory.uncheckedSimpleType(String.class);
        MapType type = typeFactory.constructMapType(Map.class, stringType, mapType);

        Map<String, Map<String, Document>> rootNode = MAPPER.readValue(dataScript, type);
        return rootNode.get(DATA_ROOT);
    }

}
