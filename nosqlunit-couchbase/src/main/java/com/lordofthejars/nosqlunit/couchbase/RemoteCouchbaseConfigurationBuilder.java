package com.lordofthejars.nosqlunit.couchbase;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RemoteCouchbaseConfigurationBuilder {

    public static class Builder {

        private String name;
        private String password = "";
        private List<URI> uris;

        private Builder() {
            uris = new ArrayList<URI>();
        }

        public static Builder start() {
            return new Builder();
        }

        public Builder bucketName(final String name) {
            this.name = name;
            return this;
        }

        public Builder bucketPassword(final String pass) {
            password = pass;
            return this;
        }

        public Builder serverUri(final String url) {
            uris.add(URI.create(url));
            return this;
        }

        public CouchbaseConfiguration build() {
            if (uris.isEmpty()) {
                uris.add(URI.create("http://localhost:8091/pools"));
            }
            return new CouchbaseConfiguration(uris, password, name);
        }
    }
}
