package com.lordofthejars.nosqlunit.redis.embedded;

import redis.clients.jedis.Jedis;

public class NoArgsJedis extends Jedis {

	private static final String LOCALHOST = "127.0.0.1";
	
	public NoArgsJedis() {
		super(LOCALHOST);
	}

}
