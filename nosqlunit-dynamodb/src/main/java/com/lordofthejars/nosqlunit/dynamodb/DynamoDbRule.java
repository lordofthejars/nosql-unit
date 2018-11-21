
package com.lordofthejars.nosqlunit.dynamodb;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfigurationBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDbConfigurationBuilder.*;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class DynamoDbRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "json";

    protected DatabaseOperation<AmazonDynamoDB> databaseOperation;

    public static class DynamoDbRuleBuilder {

        private DynamoDbConfiguration dynamoDbConfiguration;

        private Object target;

        private DynamoDbRuleBuilder() {
        }

        public static DynamoDbRuleBuilder newDynamoDbRule() {
            return new DynamoDbRuleBuilder();
        }

        public DynamoDbRuleBuilder configure(DynamoDbConfiguration dynamoDbConfiguration) {
            this.dynamoDbConfiguration = dynamoDbConfiguration;
            return this;
        }

        public DynamoDbRuleBuilder unitInstance(Object target) {
            this.target = target;
            return this;
        }

        public DynamoDbRule defaultEmbeddedDynamoDb() {
            return new DynamoDbRule(inMemoryDynamoDb().build());
        }

        public DynamoDbRule defaultSpringDynamoDb() {
            return new SpringDynamoDbRule(dynamoDb().build());
        }

        public DynamoDbRule build() {

            if (this.dynamoDbConfiguration == null) {
                throw new IllegalArgumentException("Configuration object should be provided.");
            }

            return new DynamoDbRule(dynamoDbConfiguration, target);
        }

    }

    public DynamoDbRule(DynamoDbConfiguration dynamoDbConfiguration) {
        super(dynamoDbConfiguration.getConnectionIdentifier());
        databaseOperation = new DynamoOperation(dynamoDbConfiguration);
    }

    /*
     * With JUnit 10 is impossible to get target from a Rule, it seems that future
     * versions will support it. For now constructor is approach is the only way.
     */
    public DynamoDbRule(DynamoDbConfiguration dynamoDbConfiguration, Object target) {
        super(dynamoDbConfiguration.getConnectionIdentifier());
        setTarget(target);
        databaseOperation = new DynamoOperation(dynamoDbConfiguration);
    }

    @Override
    public DatabaseOperation<AmazonDynamoDB> getDatabaseOperation() {
        return this.databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    @Override
    public void close() {
        // do nothing
    }

}
