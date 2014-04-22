package com.lordofthejars.nosqlunit.couchbase.integration;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.couchbase.CouchbaseOperation;
import com.lordofthejars.nosqlunit.couchbase.CouchbaseRule;
import com.lordofthejars.nosqlunit.couchbase.RemoteCouchbaseConfigurationBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class WhenComparingCouchbaseDataset {

    public static final String BUCKET = "test";


    @ClassRule
    public static CouchbaseRule couchbaseRule = new CouchbaseRule(RemoteCouchbaseConfigurationBuilder.Builder.start()
            .bucketName("test")
            .serverUri("http://10.0.0.3:8091/pools")
            .build());

    private static CouchbaseOperation operation;

    @BeforeClass
    public static final void startUp() {
        operation = couchBaseOperation();
    }

    private static CouchbaseOperation couchBaseOperation() {
        return (CouchbaseOperation) couchbaseRule.getDatabaseOperation();
    }

    @Before
    public void setUp() {
        operation.deleteAll();
    }

    private static final String DATASET_WITH_IDS = "{\n" +
            "    \"data\": {\n" +
            "        \"K::1\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"The Lord Of The Rings\",\n" +
            "                \"author\": {\n" +
            "                    \"name\":\"JRR Tolkien\",\n" +
            "                    \"born\":\"03-01-1892\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"T::the-lord-of-the-rings\": {\n" +
            "            \"document\": \"K::1\"\n" +
            "        },\n" +
            "        \"K::2\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"A Game Of Throne\",\n" +
            "                \"author\": {\n" +
            "                    \"name\": \"George RR Martin\",\n" +
            "                    \"born\": \"20-09-1948\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"T::a-game-of-throne\": {\n" +
            "            \"document\":\"K::2\"\n" +
            "        }\n" +
            "    }\n" +
            "}";

    private static final String EXPECTED_DATASET_WITH_DIFFERENT_NUMBER_OF_DOCUMENTS = "{\n" +
            "    \"data\": {\n" +
            "        \"K::1\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"The Lord Of The Rings\",\n" +
            "                \"author\": {\n" +
            "                    \"name\":\"JRR Tolkien\",\n" +
            "                    \"born\":\"03-01-1892\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"K::2\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"A Game Of Throne\",\n" +
            "                \"author\": {\n" +
            "                    \"name\": \"George RR Martin\",\n" +
            "                    \"born\": \"20-09-1948\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"T::a-game-of-throne\": {\n" +
            "            \"document\":\"K::2\"\n" +
            "        }\n" +
            "    }\n" +
            "}";

    private static final String EXPECTED_DATASET_WITH_DIFFERENT_DOCUMENT_ATTRIBUTES = "{\n" +
            "    \"data\": {\n" +
            "        \"K::1\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"The Lord Of The R1ngs\",\n" +
            "                \"author\": {\n" +
            "                    \"name\":\"JRR Tolkien\",\n" +
            "                    \"born\":\"03-01-1892\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"T::the-lord-of-the-rings\": {\n" +
            "            \"document\": \"K::1\"\n" +
            "        },\n" +
            "        \"K::2\": {\n" +
            "            \"document\": {\n" +
            "                \"title\": \"A Game Of Throne\",\n" +
            "                \"author\": {\n" +
            "                    \"name\": \"George RR Martin\",\n" +
            "                    \"born\": \"20-09-1948\"\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"T::a-game-of-throne\": {\n" +
            "            \"document\":\"K::2\"\n" +
            "        }\n" +
            "    }\n" +
            "}";

    @Test
    public void no_exception_should_be_thrown_if_data_is_expected() throws Throwable {
        operation.insert(toByteArrayInputStream(DATASET_WITH_IDS));
        boolean result = operation.databaseIs(toByteArrayInputStream(DATASET_WITH_IDS));
        assertThat(result, is(true));
    }

    @Test
    public void exception_should_be_thrown_with_different_number_of_documents() {
        operation.insert(toByteArrayInputStream(DATASET_WITH_IDS));
        try {
            operation.databaseIs(toByteArrayInputStream(EXPECTED_DATASET_WITH_DIFFERENT_NUMBER_OF_DOCUMENTS));
            fail("Should generate an error");
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), startsWith("Expected number of elements are 3 but insert are 4."));
        }
    }

    @Test
    public void exception_should_be_thrown_with_different_document_attribute() {
        operation.insert(toByteArrayInputStream(DATASET_WITH_IDS));
        try {
            operation.databaseIs(toByteArrayInputStream(EXPECTED_DATASET_WITH_DIFFERENT_DOCUMENT_ATTRIBUTES));
            fail("Should generate an error");
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected element # \"{\\\"title\\\":\\\"The Lord Of The R1ngs\\\"," +
                    "\\\"author\\\":{\\\"name\\\":\\\"JRR Tolkien\\\",\\\"born\\\":\\\"03-01-1892\\\"}}\" # is not found but # \"{\\\"title\\\":\\\"The Lord Of The Rings\\\",\\\"author\\\":{\\\"name\\\":\\\"JRR Tolkien\\\",\\\"born\\\":\\\"03-01-1892\\\"}}\" # was found."));
        }
    }

    private ByteArrayInputStream toByteArrayInputStream(String data) {
        return new ByteArrayInputStream(data.getBytes());
    }


}
