package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.query.ExtractedItem;
import com.marklogic.client.query.ExtractedResult;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.Optional;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.*;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

/**
 * Tests binary content handling.
 */
public class WhenMarkLogicRuleIsRegisteredThenBinary {

    /**
     * The application database available in the default installation.
     */
    private static final String TEST_DATABASE = "Documents";

    /**
     * The application port available in the default installation.
     */
    private static final int TEST_APP_PORT = 8000;

    private static final Statement NO_OP_STATEMENT = new Statement() {
        @Override
        public void evaluate() throws Throwable {
        }
    };

    @ClassRule
    public static ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();


    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(BinaryTestClass.class, "one_lorem_ipsum_wrong");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new BinaryTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(BinaryTestClass.class, "one_lorem_ipsum_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new BinaryTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(BinaryTestClass.class, "one_lorem_ipsum_delete");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new BinaryTestClass());
        marklogicStatement.evaluate();

        Optional<DocumentDescriptor> currentData = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.pdf");
        assertFalse(currentData.isPresent());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        BinaryTestClass testObject = new BinaryTestClass();
        FrameworkMethod frameworkMethod = frameworkMethod(BinaryTestClass.class, "one_lorem_ipsum_insert");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<DocumentDescriptor> currentData = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.pdf");
        assertTrue(currentData.isPresent());
        assertEquals(52990, currentData.get().getByteLength());

        FrameworkMethod frameworkMethod2 = frameworkMethod(BinaryTestClass.class, "two_lorem_ipsum_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<DocumentDescriptor> previousData = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.pdf");
        assertTrue(previousData.isPresent());
        assertEquals(52990, previousData.get().getByteLength());

        Optional<DocumentDescriptor> data = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.docx");
        assertTrue(data.isPresent());
        assertEquals(14629, data.get().getByteLength());
    }

    @Test
    public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);
        BinaryTestClass testObject = new BinaryTestClass();

        FrameworkMethod frameworkMethod = frameworkMethod(BinaryTestClass.class, "one_lorem_ipsum_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<DocumentDescriptor> currentData = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.pdf");
        assertTrue(currentData.isPresent());
        assertEquals(52990, currentData.get().getByteLength());

        FrameworkMethod frameworkMethod2 = frameworkMethod(BinaryTestClass.class, "two_lorem_ipsum_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<DocumentDescriptor> previousData = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.pdf");
        assertTrue(previousData.isPresent());
        assertEquals(52990, previousData.get().getByteLength());

        Optional<DocumentDescriptor> data = findOptionalOneByUri(marklogicConfiguration.getDatabaseClient(), "/lorem/ipsum/lorem-ipsum.docx");
        assertTrue(data.isPresent());
        assertEquals(14629, data.get().getByteLength());
    }

    private Optional<DocumentDescriptor> findOptionalOneByUri(DatabaseClient client, String uri) {
        BinaryDocumentManager documentManager = client.newBinaryDocumentManager();
        DocumentDescriptor result = documentManager.exists(uri);
        return Optional.ofNullable(result);
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

class BinaryTestClass {

    @Test
    @UsingDataSet(locations = "lorem/ipsum/wrong/lorem-ipsum.pdf", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "lorem/ipsum/wrong/lorem-ipsum-expected.pdf")
    public void one_lorem_ipsum_wrong() {
    }

    @Test
    @UsingDataSet(locations = "lorem/ipsum/lorem-ipsum.pdf", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "lorem/ipsum/lorem-ipsum-expected.pdf")
    public void one_lorem_ipsum_equal() {
    }

    @Test
    @UsingDataSet(locations = "lorem/ipsum/lorem-ipsum.pdf", loadStrategy = DELETE_ALL)
    public void one_lorem_ipsum_delete() {
    }

    @Test
    @UsingDataSet(locations = "lorem/ipsum/lorem-ipsum.pdf", loadStrategy = INSERT)
    public void one_lorem_ipsum_insert() {
    }

    @Test
    @UsingDataSet(locations = "lorem/ipsum/lorem-ipsum.docx", loadStrategy = INSERT)
    public void two_lorem_ipsum_insert() {
    }
}
