package com.lordofthejars.nosqlunit.mongodb;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;

public class InMemoryMongoDbConfigurationBuilder {

	protected static final String MOCK_HOST = "0.0.0.0";
	protected static final int MOCK_PORT = 2307;
	
	private MongoDbConfigurationBuilder mongoDbConfigurationBuilder;
	
	public static InMemoryMongoDbConfigurationBuilder inMemoryMongoDb() {
		return new InMemoryMongoDbConfigurationBuilder();
	}
	
	private InMemoryMongoDbConfigurationBuilder() {
		mongoDbConfigurationBuilder = mongoDb();
	}
	
	public InMemoryMongoDbConfigurationBuilder databaseName(String databaseName) {
		mongoDbConfigurationBuilder.databaseName(databaseName);
		return this;
	}
	
	public MongoDbConfiguration build() {
		return mongoDbConfigurationBuilder.host(MOCK_HOST).port(MOCK_PORT).build();
	}
	
}
