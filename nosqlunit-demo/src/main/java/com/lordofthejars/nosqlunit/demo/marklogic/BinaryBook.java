package com.lordofthejars.nosqlunit.demo.marklogic;

import java.util.Arrays;
import java.util.Objects;

/**
 * The title of a binary book instance is actually a document ID (URI).
 */
public class BinaryBook extends Book {

    private byte[] content;

    public BinaryBook(String title, byte[] content) {
        super(title, -1);
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinaryBook that = (BinaryBook) o;
        return getTitle().equals(that.getTitle()) &&
                Arrays.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getTitle());
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }
}
