package com.lordofthejars.nosqlunit.couchdb.integration;

import static com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb.ManagedCouchDbRuleBuilder.newManagedCouchDbRule;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.couchdb.CouchDbOperation;
import com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb;

public class WhenComparingCouchDbDataset {

	private static final String DATABASE_NAME = "test";

	static {
		System.setProperty("COUCHDB_HOME", "/usr/local");
	}
	
	private static final String COUCHDB_DATASET_WITH_IDS = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"1\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"_id\":\"2\",\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String COUCHDB_DATASET_WITH_NO_IDS = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String COUCHDB_DATASET_WITH_MIXED = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"1\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String EXPECTED_DATASET_WITH_DIFFERENT_NUMBER_OF_DOCUMENTS = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"1\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      }\n"+
			"   ]\n" + 
			"}";
	
	private static final String EXPECTED_COUCHDB_DATASET_WITH_DIFFERENT_IDS = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"3\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"_id\":\"2\",\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"1\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"_id\":\"2\",\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR_WITHOUT_IDS = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"George RR Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	private static final String EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR_AND_MIXED = "{\n" + 
			"   \"data\":[\n" + 
			"      {\n" + 
			"         \"_id\":\"1\",\n" + 
			"         \"title\":\"The Lord Of The Rings\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"JRR Tolkien\",\n" + 
			"            \"born\":\"03-01-1892\"\n" + 
			"         }\n" + 
			"      },\n" + 
			"      {\n" + 
			"         \"title\":\"A Game Of Thrones\",\n" + 
			"         \"author\":{\n" + 
			"            \"name\":\"Martin\",\n" + 
			"            \"born\":\"20-09-1948\"\n" + 
			"         }\n" + 
			"      }\n" + 
			"   ]\n" + 
			"}";
	
	@ClassRule
	public static ManagedCouchDb managedCouchDb = newManagedCouchDbRule().build();

	private static CouchDbOperation couchDbOperation;

	@BeforeClass
	public static final void startUp() {
		couchDbOperation = couchDbOperation();
	}

	@Before
	public void setUp() {
		couchDbOperation.deleteAll();
	}

	@Test
	public void no_exception_should_be_thrown_if_data_is_expected_with_ids() throws Throwable {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_IDS));
		boolean result = couchDbOperation.databaseIs(toByteArrayInputStream(COUCHDB_DATASET_WITH_IDS));
		assertThat(result, is(true));
	}

	@Test
	public void no_exception_should_be_thrown_if_data_is_expected_with_no_ids() throws Throwable {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_NO_IDS));
		boolean result = couchDbOperation.databaseIs(toByteArrayInputStream(COUCHDB_DATASET_WITH_NO_IDS));
		assertThat(result, is(true));
	}

	@Test
	public void no_exception_should_be_thrown_if_data_is_expected_with_mixed_ids() throws Throwable {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_MIXED));
		boolean result = couchDbOperation.databaseIs(toByteArrayInputStream(COUCHDB_DATASET_WITH_MIXED));
		assertThat(result, is(true));
	}
	
	@Test
	public void exception_should_be_thrown_with_different_number_of_documents() {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_IDS));

		try {
			couchDbOperation.databaseIs(toByteArrayInputStream(EXPECTED_DATASET_WITH_DIFFERENT_NUMBER_OF_DOCUMENTS));
			fail();
		} catch (NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of elements are 1 but insert are 2."));
		}

	}

	@Test
	public void exception_should_be_thrown_with_different_document_attribute() {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_IDS));

		try {
			couchDbOperation.databaseIs(toByteArrayInputStream(EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR));
			fail();
		} catch (NoSqlAssertionError e) {
			assertThat(
					e.getMessage(),
					is("Expected element # {\"_id\":\"1\",\"title\":\"The Lord Of The Rings\",\"author\":{\"name\":\"Tolkien\",\"born\":\"03-01-1892\"}} # is not found but # {\"_id\":\"1\",\"title\":\"The Lord Of The Rings\",\"author\":{\"name\":\"JRR Tolkien\",\"born\":\"03-01-1892\"}} # was found."));
		}

	}

	@Test
	public void exception_should_be_thrown_with_different_document_attribute_with_no_ids() {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_NO_IDS));

		try {
			couchDbOperation.databaseIs(toByteArrayInputStream(EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR_WITHOUT_IDS));
			fail();
		} catch (NoSqlAssertionError e) {
			assertThat(
					e.getMessage(),
					is("Expected element # {\"title\":\"The Lord Of The Rings\",\"author\":{\"name\":\"Tolkien\",\"born\":\"03-01-1892\"}} # is not found."));
		}

	}
	
	@Test
	public void exception_should_be_thrown_with_different_document_attribute_with_mixed_ids() {
		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_MIXED));

		try {
			couchDbOperation.databaseIs(toByteArrayInputStream(EXPECTED_COUCHDB_DATASET_WITH_WRONG_AUTHOR_AND_MIXED));
			fail();
		} catch (NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected element # {\"title\":\"A Game Of Thrones\",\"author\":{\"name\":\"Martin\",\"born\":\"20-09-1948\"}} # is not found."));
		}

	}
	
	@Test
	public void exception_should_be_thrown_with_different_id() {

		couchDbOperation.insert(toByteArrayInputStream(COUCHDB_DATASET_WITH_IDS));
		try {
			couchDbOperation.databaseIs(toByteArrayInputStream(EXPECTED_COUCHDB_DATASET_WITH_DIFFERENT_IDS));
			fail();
		} catch (NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Document with id 3 is not found."));
		}

	}

	private static CouchDbOperation couchDbOperation() {
		CouchDbConnector db = couchDbConnector();
		return new CouchDbOperation(db);
	}

	private static CouchDbConnector couchDbConnector() {
		HttpClient httpClient = new StdHttpClient.Builder().build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		// if the second parameter is true, the database will be created if it
		// doesn't exists
		CouchDbConnector db = dbInstance.createConnector(DATABASE_NAME, true);
		return db;
	}

	private ByteArrayInputStream toByteArrayInputStream(String data) {
		return new ByteArrayInputStream(data.getBytes());
	}

}
