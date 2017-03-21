package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public interface MongoDbConnectionCallback {

	MongoDatabase db();
	MongoClient mongoClient();
}
