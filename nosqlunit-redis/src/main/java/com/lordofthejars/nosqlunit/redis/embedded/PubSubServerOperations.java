package com.lordofthejars.nosqlunit.redis.embedded;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;

public class PubSubServerOperations {

	public void psubscribe(final JedisPubSub jedisPubSub, final byte[]... patterns) {

	}

	public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
		
	}
	
	public void subscribe(JedisPubSub jedisPubSub, byte[]... channels) {
		 
	}
	
	public Long publish(byte[] channel, byte[] message) {
		return 0L;
	}
	
}
