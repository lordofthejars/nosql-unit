package com.lordofthejars.nosqlunit.couchbase.model;

import com.couchbase.client.java.document.json.JsonObject;

public class Document {

    private JsonObject document;

    private Integer expirationSecs;

    public Document(JsonObject document, Integer expirationSecs) {
        super();

        if (document == null) {
            throw new IllegalArgumentException("Document should be provided");
        }
        this.document = document;
        this.expirationSecs = expirationSecs;
    }

    public JsonObject getDocument() {
        return document;
    }

    public Integer getExpirationSecs() {
        return expirationSecs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((document == null) ? 0 : document.hashCode());
        result = prime * result
                + ((expirationSecs == null) ? 0 : expirationSecs.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Document other = (Document) obj;
        if (document == null) {
            if (other.document != null)
                return false;
        } else if (!document.equals(other.document))
            return false;
        if (expirationSecs == null) {
            if (other.expirationSecs != null)
                return false;
        } else if (!expirationSecs.equals(other.expirationSecs))
            return false;
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Document{");
        sb.append("document=").append(document);
        sb.append(", expirationSecs=").append(expirationSecs);
        sb.append('}');
        return sb.toString();
    }
}
