
package com.lordofthejars.nosqlunit.dynamodb.integration;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfigurationBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb.InMemoryDynamoRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfiguration;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbRule;
import com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb;

public class WhenDynamoObjectIsAnnotatedWithInject {

    @ClassRule
    public static final InMemoryDynamoDb IN_MEMORY_DYNAMO_DB = newInMemoryDynamoDbRule().build();

    @Inject
    private AmazonDynamoDB dynamo;

    @Before
    public void setUp() {
        dynamo = null;
    }

    @Test
    public void dynamo_instance_used_in_rule_should_be_injected() throws Throwable {

        DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration, this);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenDynamoObjectIsAnnotatedWithInject.class,
                "dynamo_instance_used_in_rule_should_be_injected");
        Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, this);
        dynamodbStatement.evaluate();

        assertThat(dynamo, is(remoteDynamoDbRule.getDatabaseOperation().connectionManager()));

    }

    @Test
    public void dynamo_instance_used_in_rule_should_be_injected_without_this_reference() throws Throwable {

        DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenDynamoObjectIsAnnotatedWithInject.class,
                "dynamo_instance_used_in_rule_should_be_injected");
        Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, this);
        dynamodbStatement.evaluate();

        assertThat(dynamo, is(remoteDynamoDbRule.getDatabaseOperation().connectionManager()));

    }

    private FrameworkMethod frameworkMethod(Class<?> testClass, String methodName) {

        try {
            Method method = testClass.getMethod(methodName);
            return new FrameworkMethod(method);
        } catch (SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

    }

}
