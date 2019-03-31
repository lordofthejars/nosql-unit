package com.lordofthejars.nosqlunit.demo.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.ContentHandleFactory;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.marklogic.client.io.Format.BINARY;

public class BinaryBookManager extends GenericBookManager {

    private static final String SLASH = "/";

    private final ContentHandleFactory contentHandleFactory;

    public BinaryBookManager(DatabaseClient client) {
        super(client);
        contentHandleFactory = BytesHandle.newFactory();
    }

    private static String docId(Book book) {
        return book.getTitle().startsWith(SLASH) ? book.getTitle() : SLASH + book.getTitle();
    }

    @Override
    public void create(Book b) {
        if (!(b instanceof BinaryBook)) {
            throw new IllegalArgumentException();
        }
        BinaryBook book = (BinaryBook) b;
        DocumentManager documentManager = documentManager();
        DocumentWriteSet writeSet = documentManager.newWriteSet();
        ContentHandle<byte[]> contentHandle = contentHandleFactory().newHandle(byte[].class);
        contentHandle.set(book.getContent());
        writeSet.add(docId(book), contentHandle);
        documentManager.write(writeSet);
    }

    @Override
    public Book findBookById(String id) {
        List<Book> result = search(new StructuredQueryBuilder().document(id), 1);
        return result.isEmpty() ? null : result.get(0);
    }

    @Override
    public List<Book> findAllBooksInCollection(String... collections) {
        throw new UnsupportedOperationException("Operations on collections not supported for binary documents!");
    }

    @Override
    public List<Book> search(QueryDefinition query, long start) {
        List<Book> result = new ArrayList<Book>();
        DocumentManager documentManager = client.newBinaryDocumentManager();
        DocumentPage documentPage = documentManager.search(
                query,
                start
        );
        while (documentPage.hasNext()) {
            DocumentRecord documentRecord = documentPage.next();
            ContentHandle<byte[]> handle = contentHandleFactory().newHandle(byte[].class);
            handle = documentRecord.getContent(handle);
            result.add(new BinaryBook(documentRecord.getUri(), handle.get()));
        }
        return result;
    }

    @Override
    protected DocumentManager documentManager() {
        return client.newBinaryDocumentManager();
    }

    @Override
    protected Format format() {
        return BINARY;
    }

    @Override
    protected ContentHandleFactory contentHandleFactory() {
        return contentHandleFactory;
    }

    @Override
    protected String extension() {
        return "";
    }
}
