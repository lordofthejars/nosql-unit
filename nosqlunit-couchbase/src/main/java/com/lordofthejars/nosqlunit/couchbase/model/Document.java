package com.lordofthejars.nosqlunit.couchbase.model;

public class Document {

    // This is the content of the doc, in case is a json will be a map, otherwise a String
    private Object document;

    private Integer expirationMSecs;

    public Document() {
        super();
    }

    public Document(Object document, Integer expirationMSecs) {
        super();
        this.document = document;
        this.expirationMSecs = expirationMSecs;
    }

    public Object getDocument() {
        return document;
    }

    public void setDocument(Object document) {
        this.document = document;
    }

    public Integer getExpirationMSecs() {
        return expirationMSecs;
    }

    public void setExpirationMSecs(Integer expirationMSecs) {
        this.expirationMSecs = expirationMSecs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((document == null) ? 0 : document.hashCode());
        result = prime * result
                + ((expirationMSecs == null) ? 0 : expirationMSecs.hashCode());
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
        if (expirationMSecs == null) {
            if (other.expirationMSecs != null)
                return false;
        } else if (!expirationMSecs.equals(other.expirationMSecs))
            return false;
        return true;
    }

    public int calculateExpiration() {
        return expirationMSecs == null ?
                0 :
                (int) (System.currentTimeMillis() + expirationMSecs) / 1000;
    }
}
