package com.lordofthejars.nosqlunit.demo.marklogic;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.CLEAN_INSERT;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicRule.MarkLogicRuleBuilder.newMarkLogicRule;

public class WhenANewBookIsCreated {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule managedMarkLogicRule = newMarkLogicRule().defaultManagedMarkLogic();

    @Inject
    private DatabaseClient client;

    @Test
    @UsingDataSet(locations = "books.xml", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "books-expected.xml")
    public void xml_book_should_be_inserted_into_database() {
        GenericBookManager bookManager = new XmlBookManager(client);
        Book book = new Book("The Road Goes Ever On", 96);
        bookManager.create(book);
    }

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "books-expected.json")
    public void json_book_should_be_inserted_into_database() {
        GenericBookManager bookManager = new JsonBookManager(client);
        Book book = new Book("The Road Goes Ever On", 96);
        bookManager.create(book);
    }
}
