package com.lordofthejars.nosqlunit.demo.redis;

import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule;
import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.redis.ManagedRedis;
import com.lordofthejars.nosqlunit.redis.RedisRule;

public class WhenYouFindABook {

	static {
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.17");
	}

	@ClassRule
	public static ManagedRedis managedRedis = newManagedRedisRule().build();

	@Rule
	public RedisRule redisRule = newRedisRule().defaultManagedRedis();
	
	@Test
	@UsingDataSet(locations="book.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void book_should_be_returned_if_title_is_in_database() {
		
		BookManager bookManager = new BookManager(new Jedis("localhost"));
		Book findBook = bookManager.findBookByTitle("The Hobbit");
		
		assertThat(findBook, is(new Book("The Hobbit", 293)));
		
	}

}
