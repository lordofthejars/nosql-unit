package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public final class MongoDbLowLevelOps {

	public boolean assertThatConnectionIsPossible(String host, int port, int retries) throws InterruptedException, UnknownHostException, MongoException {
	
		int currentRetry = 0;
		boolean connectionIsPossible = false;
		
		Mongo server = null;
		try {
			do { 
				TimeUnit.SECONDS.sleep(3);
				server = new Mongo(host, port);
				DB db = server.getDB("admin");
				try {
					db.getStats();
					connectionIsPossible = true;
				}catch(MongoException e) {
					currentRetry++;					
				}
			}while(!connectionIsPossible && currentRetry <= retries);
		} finally {
			server.close();
		}
		
		return connectionIsPossible;
	}
	
	public void shutdown(String host, int port) {
		Mongo mongo = null;
		try {
			mongo = new Mongo(host, port);
			DB db = mongo.getDB("admin");
			CommandResult shutdownResult = db.command(new BasicDBObject(
					"shutdown", 1));
			shutdownResult.throwOnError();
		} catch (MongoException.Network e) {
			//It is ok because response could not be returned because network connection is closed.
		} catch (Throwable e) {
			throw new IllegalStateException("Mongodb could not be shutdown.", e);
		} finally {
			mongo.close();
		}
	}
	
}
