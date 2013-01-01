package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.DB;

public interface MongoDbConnectionCallback {

	DB db();
	
}
