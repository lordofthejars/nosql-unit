package com.lordofthejars.nosqlunit.demo.marklogic;


import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;
import java.util.List;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.CLEAN_INSERT;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicRule.MarkLogicRuleBuilder.newMarkLogicRule;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class WhenYouLookForAllBestsellers {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule managedMarkLogicRule = newMarkLogicRule().defaultManagedMarkLogic();

    @Inject
    private DatabaseClient client;

    @Test
    @UsingDataSet(locations = "books.xml", loadStrategy = CLEAN_INSERT)
    public void only_xml_books_in_collection_should_be_returned() {
        GenericBookManager bookManager = new XmlBookManager(client);
        List<Book> books = bookManager.findAllBooksInCollection("bestsellers");
        assertThat(books, hasSize(2));
        assertThat(books, hasItems(new Book("The Lord Of The Rings", 1299)));
        assertThat(books, not(contains(new Book("The Silmarillion the Myths and Legends of Middle Earth", 365))));
    }

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = CLEAN_INSERT)
    public void only_json_books_in_collection_should_be_returned() {
        GenericBookManager bookManager = new JsonBookManager(client);
        List<Book> books = bookManager.findAllBooksInCollection("bestsellers");
        assertThat(books, hasSize(2));
        assertThat(books, hasItems(new Book("The Lord Of The Rings", 1299)));
        assertThat(books, not(contains(new Book("The Silmarillion the Myths and Legends of Middle Earth", 365))));
    }
}
