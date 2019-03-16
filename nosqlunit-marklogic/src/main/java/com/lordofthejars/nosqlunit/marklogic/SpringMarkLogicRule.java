package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.PropertyGetter;
import com.lordofthejars.nosqlunit.util.SpringUtils;
import com.marklogic.client.DatabaseClient;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;

public class SpringMarkLogicRule extends MarkLogicRule {

    private PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<ApplicationContext>();

    private MarkLogicConfiguration marklogicConfiguration;

    public SpringMarkLogicRule(MarkLogicConfiguration marklogicConfiguration) {
        super(marklogicConfiguration);
        this.marklogicConfiguration = marklogicConfiguration;
    }

    public SpringMarkLogicRule(MarkLogicConfiguration marklogicConfiguration, Object object) {
        super(marklogicConfiguration, object);
        this.marklogicConfiguration = marklogicConfiguration;
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
        databaseOperation = new MarkLogicOperation(definedDatabaseClient(testObject), marklogicConfiguration);
        return super.apply(base, method, testObject);
    }

    @Override
    public void close() {
        // DO NOT CLOSE the connection (Spring will do it when destroying the context)
    }

    private DatabaseClient definedDatabaseClient(Object testObject) {
        ApplicationContext applicationContext = propertyGetter.propertyByType(testObject, ApplicationContext.class);
        DatabaseClient databaseClient = SpringUtils.getBeanOfType(applicationContext, DatabaseClient.class);
        if (databaseClient == null) {
            throw new IllegalArgumentException(
                    "At least one DatabaseClient instance should be defined into Spring Application Context.");
        }
        return databaseClient;
    }

}
