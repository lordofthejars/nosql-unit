package com.lordofthejars.nosqlunit.demo.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class PersonManager {
	
	private ColumnFamilyTemplate<String, String> template;
	
	public PersonManager(String clusterName, String keyspaceName, String host) {
		Cluster cluster = HFactory.getOrCreateCluster(clusterName, host);
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		
        template = new ThriftColumnFamilyTemplate<String, String>(keyspace,
        		"personFamilyName", 
                                                               StringSerializer.get(),        
                                                               StringSerializer.get());
		
	}
	
	public String getCarByPersonName(String name) {
		ColumnFamilyResult<String, String> queryColumns = template.queryColumns(name);
		return queryColumns.getString("car");
	}
	
	public void updateCarByPersonName(String name, String car) {
		ColumnFamilyUpdater<String, String> createUpdater = template.createUpdater(name);
		createUpdater.setString("car", car);
		
		template.update(createUpdater);
	}
	
}
