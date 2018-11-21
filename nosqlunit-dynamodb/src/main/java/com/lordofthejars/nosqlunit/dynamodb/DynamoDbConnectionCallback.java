
package com.lordofthejars.nosqlunit.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public interface DynamoDbConnectionCallback {

    AmazonDynamoDB dbClient();
}
