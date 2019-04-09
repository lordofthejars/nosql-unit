package com.lordofthejars.nosqlunit.demo.marklogic;

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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class WhenYouFindBooksById {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule managedMarkLogicRule = newMarkLogicRule().defaultManagedMarkLogic();

    @Inject
    private DatabaseClient client;

    @Test
    @UsingDataSet(locations = "books.xml", loadStrategy = CLEAN_INSERT)
    public void xml_book_with_properties_should_be_returned() {
        GenericBookManager bookManager = new XmlBookManager(client);
        Book book = bookManager.findBookById("The Hobbit");
        assertThat(book, is(notNullValue()));
        assertThat(book.getTitle(), is("The Hobbit"));
    }

    @Test
    @UsingDataSet(locations = "books.json", loadStrategy = CLEAN_INSERT)
    public void json_book_with_properties_should_be_returned() {
        GenericBookManager bookManager = new JsonBookManager(client);
        Book book = bookManager.findBookById("The Hobbit");
        assertThat(book, is(notNullValue()));
        assertThat(book.getTitle(), is("The Hobbit"));
    }

    @Test
    @UsingDataSet(locations = {
            "books/Lorem Ipsum.pdf"
            , "books/Lorem Ipsum.docx"
            , "books/Lorem Ipsum.txt"
    }, loadStrategy = CLEAN_INSERT)
    public void binary_book_with_id_should_be_returned() {
        GenericBookManager bookManager = new BinaryBookManager(client);
        Book book = bookManager.findBookById("/books/Lorem Ipsum.txt");
        assertThat(book, is(notNullValue()));
        assertThat(book.getTitle(), is("/books/Lorem Ipsum.txt"));
    }
}
