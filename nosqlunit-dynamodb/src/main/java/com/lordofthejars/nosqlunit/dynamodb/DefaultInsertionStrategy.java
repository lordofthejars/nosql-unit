
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.IOException;
import java.io.InputStream;

public class DefaultInsertionStrategy implements DynamoInsertionStrategy {

    @Override
    public void insert(DynamoDbConnectionCallback connection, InputStream dataset) throws IOException {
        DynamoDBOperation.insertData(connection.dbClient(), dataset);
    }

}
