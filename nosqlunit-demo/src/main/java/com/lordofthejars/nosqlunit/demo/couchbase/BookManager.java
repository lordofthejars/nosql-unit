package com.lordofthejars.nosqlunit.demo.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.lordofthejars.nosqlunit.demo.model.Book;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import java.io.IOException;

public class BookManager {

    private Bucket client;

    public BookManager(final Bucket client) {
        this.client = client;
    }

    public Book findByKey(final String key) throws IOException {
        final JsonDocument jsonDocument = client.get(key);

        final JsonObject content = jsonDocument.content();
        return new Book(content.getString("title"), content.getInt("numberOfPages"));
    }

}
