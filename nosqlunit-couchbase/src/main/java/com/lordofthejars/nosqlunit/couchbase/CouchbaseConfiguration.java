package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import org.awaitility.Awaitility;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class CouchbaseConfiguration extends AbstractJsr330Configuration {

    private final List<URI> urlList;
    private String clusterUsername;
    private String clusterPassword;


    private final String bucketPassword;

    private final String bucketName;
    private final boolean createBucket;

    private Bucket bucket;
    private CouchbaseCluster couchbaseCluster;

    public CouchbaseConfiguration(final List<URI> urlList, final String bucketPassword, final String bucketName, final boolean createBucket) {
        this.urlList = urlList;
        this.bucketPassword = bucketPassword;
        this.bucketName = bucketName;
            this.couchbaseCluster = CouchbaseCluster.create(
                    urlList.stream().map(URI::toString).collect(Collectors.toList())
            );
        this.createBucket = createBucket;
    }

    private void connectToBucket() {

        if (createBucket) {
            final ClusterManager clusterManager = couchbaseCluster.clusterManager(this.clusterUsername, this.clusterPassword);
            if (! clusterManager.hasBucket(this.bucketName)) {
                // Create Bucket
                final DefaultBucketSettings defaultBucketSettings = DefaultBucketSettings.builder()
                        .enableFlush(true)
                        .name(bucketName).build();

                clusterManager.insertBucket(defaultBucketSettings);

                await()
                        .atMost(30, TimeUnit.SECONDS)
                        .until(() -> clusterManager.hasBucket(bucketName));

            }
        }

        if (this.bucketPassword != null) {
            this.bucket = couchbaseCluster.openBucket(this.bucketName, this.bucketPassword);
        } else {
            this.bucket = couchbaseCluster.openBucket(this.bucketName);
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

    public void setClusterUsername(String clusterUsername) {
        this.clusterUsername = clusterUsername;
    }

    public void setClusterPassword(String clusterPassword) {
        this.clusterPassword = clusterPassword;
    }

    public Bucket getBucket() {
        if (bucket == null) {
            connectToBucket();
        }
        return bucket;
    }

    public CouchbaseCluster getCouchbaseCluster() {
        return couchbaseCluster;
    }
}
