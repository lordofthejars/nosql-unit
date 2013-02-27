package com.lordofthejars.nosqlunit.mongodb;

import static ch.lambdaj.collection.LambdaCollections.with;
import static org.hamcrest.CoreMatchers.anything;

import java.util.Map;

import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;

import com.lordofthejars.nosqlunit.core.PropertyGetter;
import com.mongodb.Mongo;

public class SpringMongoDbRule extends MongoDbRule {

	private PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<ApplicationContext>();

	private MongoDbConfiguration mongoDbConfiguration;
	
	public SpringMongoDbRule(MongoDbConfiguration mongoDbConfiguration) {
		super(mongoDbConfiguration);
		this.mongoDbConfiguration = mongoDbConfiguration;
	}

	public SpringMongoDbRule(MongoDbConfiguration mongoDbConfiguration, Object object) {
		super(mongoDbConfiguration, object);
		this.mongoDbConfiguration = mongoDbConfiguration;
	}
	
	@Override
	public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
		this.databaseOperation = new MongoOperation(definedMongo(testObject), this.mongoDbConfiguration);
		return super.apply(base, method, testObject);
	}
	
	private Mongo definedMongo(Object testObject) {
		ApplicationContext applicationContext = propertyGetter.propertyByType(testObject, ApplicationContext.class);

		Map<String, Mongo> beansOfType = applicationContext.getBeansOfType(Mongo.class);
		Mongo mongo = with(beansOfType).values().first(anything());

		if (mongo == null) {
			throw new IllegalArgumentException(
					"At least one Mongo instance should be defined into Spring Application Context.");
		}

		return mongo;

	}
	
}
