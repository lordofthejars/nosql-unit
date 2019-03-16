package com.lordofthejars.nosqlunit.marklogic;

import com.marklogic.client.DatabaseClient;

public interface MarkLogicConnectionCallback {
    DatabaseClient databaseClient();
}
