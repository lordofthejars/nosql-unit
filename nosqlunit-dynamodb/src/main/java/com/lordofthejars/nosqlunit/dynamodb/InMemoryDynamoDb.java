
package com.lordofthejars.nosqlunit.dynamodb;

import org.junit.rules.ExternalResource;

public class InMemoryDynamoDb extends ExternalResource {

    protected InMemoryDynamoDbLifecycleManager inMemoryDynamoDbLifecycleManager = null;

    private InMemoryDynamoDb() {
        super();
    }

    public static class InMemoryDynamoRuleBuilder {

        private InMemoryDynamoDbLifecycleManager inMemoryDynamoDbLifecycleManager;

        private InMemoryDynamoRuleBuilder() {
            this.inMemoryDynamoDbLifecycleManager = new InMemoryDynamoDbLifecycleManager();
        }

        public static InMemoryDynamoRuleBuilder newInMemoryDynamoDbRule() {
            return new InMemoryDynamoRuleBuilder();
        }

        public InMemoryDynamoRuleBuilder targetPath(String targetPath) {
            this.inMemoryDynamoDbLifecycleManager.setTargetPath(targetPath);
            return this;
        }

        public InMemoryDynamoDb build() {

            if (this.inMemoryDynamoDbLifecycleManager.getTargetPath() == null) {
                throw new IllegalArgumentException("No Path to Embedded Infinispan is provided.");
            }

            InMemoryDynamoDb inMemoryDynamoDb = new InMemoryDynamoDb();
            inMemoryDynamoDb.inMemoryDynamoDbLifecycleManager = this.inMemoryDynamoDbLifecycleManager;

            return inMemoryDynamoDb;

        }

    }

    @Override
    public void before() throws Throwable {
        inMemoryDynamoDbLifecycleManager.startEngine();
    }

    @Override
    public void after() {
        inMemoryDynamoDbLifecycleManager.stopEngine();
    }

}
