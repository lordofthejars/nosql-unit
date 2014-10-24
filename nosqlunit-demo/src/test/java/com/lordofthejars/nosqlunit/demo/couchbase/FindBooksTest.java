package com.lordofthejars.nosqlunit.demo.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.couchbase.CouchbaseRule;
import com.lordofthejars.nosqlunit.couchbase.RemoteCouchbaseConfigurationBuilder;
import com.lordofthejars.nosqlunit.demo.model.Book;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FindBooksTest {

    @Inject
    CouchbaseClient client;

    @Rule
    public CouchbaseRule couchbaseRule = new CouchbaseRule(RemoteCouchbaseConfigurationBuilder.Builder.start()
        .bucketName("test")
        .serverUri("http://10.0.0.3:8091/pools")
        .build());

    @Inject
    private CouchbaseClient couchbaseClient;

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "books.json")
    public void should_find_existing_book_by_title() {
        final BookManager bookManager = new BookManager(couchbaseClient);
        final Book book = bookManager.findBookByTitle("The Hobbit");

        assertThat(book.getTitle(), is("The Hobbit"));
        assertThat(book.getNumberOfPages(), is(293));
    }

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "books.json")
    public void should_find_existing_book_by_id() {
        final BookManager bookManager = new BookManager(couchbaseClient);
        final Book book = bookManager.findById(1l);

        assertThat(book.getTitle(), is("The Hobbit"));
        assertThat(book.getNumberOfPages(), is(293));
    }

}

