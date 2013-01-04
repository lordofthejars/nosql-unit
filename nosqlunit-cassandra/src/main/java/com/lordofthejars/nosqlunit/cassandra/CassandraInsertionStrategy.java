package com.lordofthejars.nosqlunit.cassandra;

import com.lordofthejars.nosqlunit.core.InsertionStrategy;

public interface CassandraInsertionStrategy extends InsertionStrategy<CassandraConnectionCallback> {

	String getKeyspaceName();
	
}
