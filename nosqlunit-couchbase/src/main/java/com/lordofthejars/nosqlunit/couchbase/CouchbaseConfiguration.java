package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.util.List;

@Getter
@Slf4j
public class CouchbaseConfiguration extends AbstractJsr330Configuration {

    private final List<URI> urlList;

    private final String bucketPassword;

    private final String bucketName;

    private CouchbaseClient client;

    @SneakyThrows(IOException.class)
    public CouchbaseConfiguration(final List<URI> urlList, final String bucketPassword, final String bucketName) {
        this.urlList = urlList;
        this.bucketPassword = bucketPassword;
        this.bucketName = bucketName;
        client = new CouchbaseClient(urlList, bucketName, bucketPassword);
    }

}
