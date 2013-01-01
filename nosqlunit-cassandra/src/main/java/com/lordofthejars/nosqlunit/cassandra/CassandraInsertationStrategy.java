package com.lordofthejars.nosqlunit.cassandra;

import com.lordofthejars.nosqlunit.core.InsertationStrategy;

public interface CassandraInsertationStrategy extends InsertationStrategy<CassandraConnectionCallback> {

	String getKeyspaceName();
	
}
