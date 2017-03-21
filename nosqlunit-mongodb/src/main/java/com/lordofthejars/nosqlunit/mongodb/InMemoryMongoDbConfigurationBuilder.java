package com.lordofthejars.nosqlunit.mongodb;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.MongoClient;

public class InMemoryMongoDbConfigurationBuilder {

	
	private MongoDbConfiguration mongoDbConfiguration;
	
	public static InMemoryMongoDbConfigurationBuilder inMemoryMongoDb() {
		return new InMemoryMongoDbConfigurationBuilder();
	}
	
	private InMemoryMongoDbConfigurationBuilder() {
		this.mongoDbConfiguration = new MongoDbConfiguration();
	}
	
	public InMemoryMongoDbConfigurationBuilder databaseName(String databaseName) {
		this.mongoDbConfiguration.setDatabaseName(databaseName);
		return this;
	}
	
	public InMemoryMongoDbConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.mongoDbConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public MongoDbConfiguration build() {
		
		MongoClient embeddedMongo = EmbeddedMongoInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		if(embeddedMongo == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedMongo rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		if(this.mongoDbConfiguration.getDatabaseName() == null) {
			throw FailureHandler.createIllegalStateFailure("There is no database defined.");
		}
		
		this.mongoDbConfiguration.setMongo(embeddedMongo);
		return this.mongoDbConfiguration;
		
	}
	
	public MongoDbConfiguration buildFromTargetPath(String targetPath) {
		
		MongoClient embeddedMongo = EmbeddedMongoInstancesFactory.getInstance().getEmbeddedByTargetPath(targetPath);
		
		if(embeddedMongo == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedMongo rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		if(this.mongoDbConfiguration.getDatabaseName() == null) {
			throw FailureHandler.createIllegalStateFailure("There is no database defined.");
		}
		
		this.mongoDbConfiguration.setMongo(embeddedMongo);
		return this.mongoDbConfiguration;
		
	}
	
}
