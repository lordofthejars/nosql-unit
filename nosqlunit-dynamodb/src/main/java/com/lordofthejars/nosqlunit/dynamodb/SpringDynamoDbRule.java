
package com.lordofthejars.nosqlunit.dynamodb;

import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.PropertyGetter;
import com.lordofthejars.nosqlunit.util.SpringUtils;

public class SpringDynamoDbRule extends DynamoDbRule {

    private PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<>();

    private DynamoDbConfiguration dynamoDbConfiguration;

    public SpringDynamoDbRule(DynamoDbConfiguration dynamoDbConfiguration) {
        super(dynamoDbConfiguration);
        this.dynamoDbConfiguration = dynamoDbConfiguration;
    }

    public SpringDynamoDbRule(DynamoDbConfiguration dynamoDbConfiguration, Object object) {
        super(dynamoDbConfiguration, object);
        this.dynamoDbConfiguration = dynamoDbConfiguration;
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
        this.databaseOperation = new DynamoOperation(definedDynamo(testObject), this.dynamoDbConfiguration);
        return super.apply(base, method, testObject);
    }

    @Override
    public void close() {
        // DO NOT CLOSE the connection (Spring will do it when destroying the context)
    }

    private AmazonDynamoDB definedDynamo(Object testObject) {
        ApplicationContext applicationContext = propertyGetter.propertyByType(testObject, ApplicationContext.class);

        AmazonDynamoDB dynamo = SpringUtils.getBeanOfType(applicationContext, AmazonDynamoDB.class);

        if (dynamo == null) {
            throw new IllegalArgumentException(
                    "At least one AmazonDynamoDB instance should be defined into Spring Application Context.");
        }
        return dynamo;
    }

}
