package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.lordofthejars.nosqlunit.core.IOUtils;
import com.lordofthejars.nosqlunit.couchbase.model.Document;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Bucket bucket;

    public DataLoader(Bucket bucket) {
        super();
        this.bucket = bucket;
    }

    public void load(final InputStream dataScript) {
        final Map<String, Document> documentsIterator = getDocuments(dataScript);
        insertDocuments(documentsIterator);
    }

    private void insertDocuments(final Map<String, Document> documentsIterator) {
        for (final Map.Entry<String, Document> documentEntry : documentsIterator.entrySet()) {
            final Document document = documentEntry.getValue();
            final JsonDocument jsonDocument = JsonDocument.create(documentEntry.getKey(), document.getExpirationSecs(), document.getDocument());
            bucket.upsert(jsonDocument, 15, TimeUnit.SECONDS);
        }
    }

    public static Map<String, Document> getDocuments(final InputStream dataScript) {

        try {
            final JsonObject jsonObject = JsonObject.fromJson(IOUtils.readFullStream(dataScript));
            final JsonArray data = jsonObject.getArray("data");
            return StreamSupport.stream(data.spliterator(), false)
                    .map(o -> (JsonObject) o)
                    .collect(Collectors.toMap(o -> o.getString("key"),
                            o -> new Document(o.getObject("document"), o.getInt("expirationSecs"))));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

}
