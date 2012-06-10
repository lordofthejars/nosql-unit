package com.lordofthejars.nosqlunit.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.net.UnknownHostException;

import org.junit.Test;

import com.mongodb.MongoException;

public class AssertingThatConnectionIsPossible {

	@Test
	public void should_return_false_if_no_server_is_up() throws UnknownHostException, MongoException, InterruptedException {
		MongoDbLowLevelOps mongoDbLowLevelOps = new MongoDbLowLevelOps();
		boolean isConnectionPossible = mongoDbLowLevelOps.assertThatConnectionIsPossible(1);
		assertThat(isConnectionPossible, is(false));
	}
	
}
