package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDbLowLevelOps {

	public void assertThatConnectionIsPossible() throws InterruptedException, UnknownHostException, MongoException {
	
		Mongo server = null;
		try {
			while (server == null) {
				TimeUnit.SECONDS.sleep(3);
				server = new Mongo();
				DB db = server.getDB("admin");
				db.getStats();
			}
		} finally {
			server.close();
		}
		
	}
	
	public void shutdown() {
		Mongo mongo = null;
		try {
			mongo = new Mongo();
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
