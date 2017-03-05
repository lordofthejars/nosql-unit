package com.lordofthejars.nosqlunit.demo.couchbase;

import com.couchbase.client.java.Bucket;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.couchbase.CouchbaseRule;
import com.lordofthejars.nosqlunit.couchbase.RemoteCouchbaseConfigurationBuilder;
import com.lordofthejars.nosqlunit.demo.model.Book;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FindBooksTest {

    @Rule
    public CouchbaseRule couchbaseRule = new CouchbaseRule(RemoteCouchbaseConfigurationBuilder.Builder.start()
        .bucketName("default")
        .bucketPassword("")
        .serverHost("192.168.99.100")
        .build());

    @Inject
    private Bucket couchbaseClient;

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = LoadStrategyEnum.INSERT)
    public void should_find_existing_book_by_id() throws IOException {
        final BookManager bookManager = new BookManager(couchbaseClient);
        final Book book = bookManager.findByKey("Hobbit");

        assertThat(book.getTitle(), is("The Hobbit"));
        assertThat(book.getNumberOfPages(), is(293));
    }

}

