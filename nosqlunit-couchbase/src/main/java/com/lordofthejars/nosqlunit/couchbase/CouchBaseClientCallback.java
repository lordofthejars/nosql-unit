package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;

public interface CouchBaseClientCallback {

    Bucket couchBaseBucket();
}
