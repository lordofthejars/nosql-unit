package com.lordofthejars.nosqlunit.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDbConfigurationBuilder.inMemoryMongoDb;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

import com.mongodb.DBPort;

public class WhenMongoDbConfigurationIsCreated {

	@Test
	public void parameters_should_be_compatible_with_jmockmongo() {
		
		MongoDbConfiguration embeddedConfiguration = inMemoryMongoDb().databaseName("test").build();
		assertThat(embeddedConfiguration.getHost(), is(InMemoryMongoDbConfigurationBuilder.MOCK_HOST));
		assertThat(embeddedConfiguration.getPort(), is(InMemoryMongoDbConfigurationBuilder.MOCK_PORT));
		assertThat(embeddedConfiguration.getDatabaseName(), is("test"));
		
	}

	@Test
	public void managed_parameter_values_should_contain_default_values() {
		MongoDbConfiguration managedConfiguration = mongoDb().databaseName("test").build();
		
		assertThat(managedConfiguration.getHost(), is("localhost"));
		assertThat(managedConfiguration.getPort(), is(DBPort.PORT));
		assertThat(managedConfiguration.getDatabaseName(), is("test"));
		
	}
	
}
