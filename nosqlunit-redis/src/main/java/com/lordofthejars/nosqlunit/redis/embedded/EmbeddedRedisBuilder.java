package com.lordofthejars.nosqlunit.redis.embedded;

import com.lordofthejars.nosqlunit.proxy.RedirectProxy;

import redis.clients.jedis.Jedis;

public class EmbeddedRedisBuilder {

	public Jedis createEmbeddedJedis() {
		return RedirectProxy.createProxy(NoArgsJedis.class, new EmbeddedJedis());
	}
	
}
