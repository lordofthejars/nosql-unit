package com.lordofthejars.nosqlunit.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.mongodb.DBPort;

public class WhenMongoDbConfigurationIsCreated {

	@Test
	public void managed_parameter_values_should_contain_default_values() {
		MongoDbConfiguration managedConfiguration = mongoDb().databaseName("test").build();
		
		assertThat(managedConfiguration.getHost(), is("localhost"));
		assertThat(managedConfiguration.getPort(), is(DBPort.PORT));
		assertThat(managedConfiguration.getDatabaseName(), is("test"));
		
	}
	
}
