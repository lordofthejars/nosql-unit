package com.lordofthejars.nosqlunit.mongodb;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDbConfigurationBuilder.inMemoryMongoDb;

import org.junit.Test;

public class WhenEmbeddedMongoDbConfigurationIsCreated {

	@Test
	public void parameters_should_be_compatible_with_jmockmongo() {
		
		MongoDbConfiguration embeddedConfiguration = inMemoryMongoDb().databaseName("test").build();
		assertThat(embeddedConfiguration.getHost(), is(InMemoryMongoDbConfigurationBuilder.MOCK_HOST));
		assertThat(embeddedConfiguration.getPort(), is(InMemoryMongoDbConfigurationBuilder.MOCK_PORT));
		
	}

}
