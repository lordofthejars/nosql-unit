
package com.lordofthejars.nosqlunit.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.util.EmbeddedInstances;

public class EmbeddedDynamoInstancesFactory {

    private static EmbeddedInstances<AmazonDynamoDB> embeddedInstances;

    private EmbeddedDynamoInstancesFactory() {
        super();
    }

    public static synchronized EmbeddedInstances<AmazonDynamoDB> getInstance() {
        if (embeddedInstances == null) {
            embeddedInstances = new EmbeddedInstances<>();
        }

        return embeddedInstances;
    }

}
