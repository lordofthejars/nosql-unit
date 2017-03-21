package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.PropertyGetter;
import com.lordofthejars.nosqlunit.util.SpringUtils;
import com.mongodb.MongoClient;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;

public class SpringMongoDbRule extends MongoDbRule {

    private PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<ApplicationContext>();

    private MongoDbConfiguration mongoDbConfiguration;

    public SpringMongoDbRule(MongoDbConfiguration mongoDbConfiguration) {
        super(mongoDbConfiguration);
        this.mongoDbConfiguration = mongoDbConfiguration;
    }

    public SpringMongoDbRule(MongoDbConfiguration mongoDbConfiguration, Object object) {
        super(mongoDbConfiguration, object);
        this.mongoDbConfiguration = mongoDbConfiguration;
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
        this.databaseOperation = new MongoOperation(definedMongo(testObject), this.mongoDbConfiguration);
        return super.apply(base, method, testObject);
    }

    private MongoClient definedMongo(Object testObject) {
        ApplicationContext applicationContext = propertyGetter.propertyByType(testObject, ApplicationContext.class);

        MongoClient mongo = SpringUtils.getBeanOfType(applicationContext, MongoClient.class);

        if (mongo == null) {
            throw new IllegalArgumentException(
                    "At least one Mongo instance should be defined into Spring Application Context.");
        }
        return mongo;
    }

}
