package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class CouchbaseConfiguration extends AbstractJsr330Configuration {

    private final List<URI> urlList;

    private final String bucketPassword;

    private final String bucketName;

    private CouchbaseClient client;

    public CouchbaseConfiguration(final List<URI> urlList, final String bucketPassword, final String bucketName) {
        this.urlList = urlList;
        this.bucketPassword = bucketPassword;
        this.bucketName = bucketName;
        try {
            client = new CouchbaseClient(urlList, bucketName, bucketPassword);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public List<URI> getUrlList() {
        return urlList;
    }

    public String getBucketPassword() {
        return bucketPassword;
    }

    public String getBucketName() {
        return bucketName;
    }

    public CouchbaseClient getClient() {
        return client;
    }
    
}
