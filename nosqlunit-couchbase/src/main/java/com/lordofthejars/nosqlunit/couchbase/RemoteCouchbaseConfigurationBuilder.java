package com.lordofthejars.nosqlunit.couchbase;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RemoteCouchbaseConfigurationBuilder {

    public static class Builder {

        private String name;
        private String password = "";
        private String clusterUsername;
        private String clusterPassword;
        private List<URI> uris;
        private boolean createBucket = false;

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

        public Builder serverHost(final String url) {
            uris.add(URI.create(url));
            return this;
        }

        public Builder createBucket(boolean createBucket) {
            this.createBucket = createBucket;
            return this;
        }

        public Builder clusterAuth(String clusterUsername, String clusterPassword) {
            this.clusterUsername = clusterUsername;
            this.clusterPassword = clusterPassword;

            return this;
        }

        public CouchbaseConfiguration build() {
            if (uris.isEmpty()) {
                uris.add(URI.create("localhost"));
            }
            final CouchbaseConfiguration couchbaseConfiguration = new CouchbaseConfiguration(uris, password, name, createBucket );
            couchbaseConfiguration.setClusterUsername(clusterUsername);
            couchbaseConfiguration.setClusterPassword(clusterPassword);

            return couchbaseConfiguration;
        }
    }
}
