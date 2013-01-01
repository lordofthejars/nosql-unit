package com.lordofthejars.nosqlunit.cassandra;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

public interface CassandraConnectionCallback {

	Cluster cluster();
	Keyspace keyspace();
	CassandraConfiguration cassandraConfiguration();
	
}
