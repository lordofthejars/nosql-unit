package com.lordofthejars.nosqlunit.demo.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.demo.model.Book;
import lombok.SneakyThrows;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class BookManager {

    public static final ObjectMapper mapper = new ObjectMapper();

    private static final Pattern NON_LATIN = Pattern.compile("[^\\w-]");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]");

    private CouchbaseClient client;

    public BookManager(final CouchbaseClient client) {
        this.client = client;
    }

    public OperationFuture<Boolean> create(final Book book) throws ExecutionException, InterruptedException {
        /*
        Best way to have search capabilities is by another document on couchbase, so we are adding it first and then
        the proper document.
          */
        final String key = nextKey();
        final String value = toTitleKey(book.getTitle());
        final OperationFuture<Boolean> future = client.set(value, key);
        return future.addListener(new OperationCompletionListener() {
            @Override
            public void onComplete(final OperationFuture<?> operationFuture) throws Exception {
                client.set(key, mapper.writeValueAsBytes(book));
            }
        });
    }

    @SneakyThrows(IOException.class)
    public Book findBookByTitle(final String title) {
        final String key = ((String) client.get(toTitleKey(title))).replaceAll("\"","");


        if (key == null) {
            throw new IllegalStateException("Cannot find the key object for the campaign with title: " + title);
        }

        final String json = (String) client.get(key);
        return mapper.readValue(json, Book.class);
    }

    @SneakyThrows(IOException.class)
    public Book findById(final Long id) {
        final String json = (String) client.get(toIdKey(String.valueOf(id)));
        return mapper.readValue(json, Book.class);
    }

    private String toTitleKey(final String title) {
        return "T::" + toSlug(title);
    }

    private String nextKey() {
        // TODO change this for a proper autonumerical done by couchbase incr, maybe get rid of it via nosqlunit.
        return toIdKey(UUID.randomUUID().toString());
    }

    private String toIdKey(final String id) {
        return "K::" + id;
    }

    public static String toSlug(final String input) {
        final String nonwhites = WHITESPACE.matcher(input).replaceAll("-");
        final String normalized = Normalizer.normalize(nonwhites, Normalizer.Form.NFD);
        final String slug = NON_LATIN.matcher(normalized).replaceAll("");
        return slug.toLowerCase(Locale.ENGLISH);
    }

}
