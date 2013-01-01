package com.lordofthejars.nosqlunit.cassandra;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class DefaultCassandraComparisionStrategy implements CassandraComparisionStrategy {

	@Override
	public boolean compare(CassandraConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
			Throwable {
		CassandraAssertion.strictAssertEquals(new InputStreamJsonDataSet(dataset), connection.cluster(), connection.keyspace());
		return true;
	}

}
