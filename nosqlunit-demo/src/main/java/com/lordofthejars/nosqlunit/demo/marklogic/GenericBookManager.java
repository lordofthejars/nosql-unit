package com.lordofthejars.nosqlunit.demo.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentUriTemplate;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;

import java.util.ArrayList;
import java.util.List;

public abstract class GenericBookManager {

    protected static final String BOOKS_DIRECTORY = "/books/";

    protected DatabaseClient client;

    public GenericBookManager(DatabaseClient client) {
        this.client = client;
    }

    public void create(Book book) {
        DocumentManager documentManager = documentManager();
        DocumentUriTemplate template = documentManager
                .newDocumentUriTemplate(book.getTitle() + extension())
                .withDirectory(BOOKS_DIRECTORY)
                .withFormat(format());
        ContentHandle<Book> contentHandle = contentHandleFactory().newHandle(Book.class);
        contentHandle.set(book);
        documentManager.create(template, contentHandle);
    }

    public Book findBookById(String id) {
        List<Book> result = search(new StructuredQueryBuilder().document(BOOKS_DIRECTORY + id + extension()), 1);
        return result.isEmpty() ? null : result.get(0);
    }

    public List<Book> findAllBooksInCollection(String... collections) {
        return search(new StructuredQueryBuilder().collection(collections), 1);
    }

    public List<Book> findAllBooks() {
        return search(new StructuredQueryBuilder().directory(true, BOOKS_DIRECTORY), 1);
    }

    public List<Book> search(QueryDefinition query, long start) {
        List<Book> result = new ArrayList<Book>();
        XMLDocumentManager documentManager = client.newXMLDocumentManager();
        DocumentPage documentPage = documentManager.search(
                query,
                start
        );
        while (documentPage.hasNext()) {
            ContentHandle<Book> handle = contentHandleFactory().newHandle(Book.class);
            handle = documentPage.nextContent(handle);
            result.add(handle.get());
        }
        return result;
    }

    protected abstract DocumentManager documentManager();

    protected abstract Format format();

    protected abstract ContentHandleFactory contentHandleFactory();

    private String extension() {
        return "." + format().name().toLowerCase();
    }
}
