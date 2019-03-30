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
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

public class WhenYouFindAllBooks {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule managedMarkLogicRule = newMarkLogicRule().defaultManagedMarkLogic();

    @Inject
    private DatabaseClient client;

    @Test
    @UsingDataSet(locations = "books.xml", loadStrategy = CLEAN_INSERT)
    public void all_xml_books_should_be_returned() {
        GenericBookManager bookManager = new XmlBookManager(client);
        List<Book> books = bookManager.findAllBooks();
        assertThat(books, hasItems(new Book("The Hobbit", 293)));
    }

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = CLEAN_INSERT)
    public void all_json_books_should_be_returned() {
        GenericBookManager bookManager = new JsonBookManager(client);
        List<Book> books = bookManager.findAllBooks();
        assertThat(books, hasItems(new Book("The Hobbit", 293)));
    }
}
