package com.lordofthejars.nosqlunit.redis.embedded;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class EmbeddedRedisBuilderBehaviour {

	
	@Test
	public void should_create_an_embedded_jedis_instance() {
		
		EmbeddedRedisBuilder embeddedRedisBuilder = new EmbeddedRedisBuilder();
		
		Jedis embeddedJedis = embeddedRedisBuilder.createEmbeddedJedis();
		embeddedJedis.set("Name", "Alex");
		
		assertThat(embeddedJedis.get("Name"), is("Alex"));
		
		
	}
}
