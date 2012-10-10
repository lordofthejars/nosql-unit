package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;

import com.lordofthejars.nosqlunit.redis.embedded.ExpirationDatatypeOperations.TtlState;

public class EmbeddedJedisUsedWithByteArray {

	private static final int SLEEP_IN_MILLIS = 1200;
	private static final byte[] JIVE = "Jive".getBytes();
	private static final byte[] MY_JUANITA = "My Juanita".getBytes();
	private static final byte[] ROCK_THIS_TOWN = "Rock This Town".getBytes();
	private static final byte[] CONCAT_JIVE = "My JuanitaRock This Town".getBytes();
	private static final byte[] SONG = "Song".getBytes();

	private static final byte[] WALTZ = "Waltz".getBytes();
	private static final byte[] DARK_WALTZ = "Dark Waltz".getBytes();

	private static final byte[] DURATION = "12".getBytes();

	private EmbeddedJedis embeddedJedis;

	@Before
	public void setUp() {
		embeddedJedis = new EmbeddedJedis();
	}

	@Test
	public void set_should_set_string_value() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void set_should_remove_any_previous_key() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(MY_JUANITA));
		assertThat(embeddedJedis.listDatatypeOperations.llen(JIVE), is(0L));

	}

	@Test
	public void set_should_remove_expiration_time() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 10, TimeUnit.SECONDS);

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_MANAGED));

	}

	@Test
	public void get_should_get_inserted_string_element() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void get_should_return_null_if_key_not_present() {
		assertThat(embeddedJedis.get(JIVE), is(nullValue()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void get_should_throw_an_exception_if_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);
		embeddedJedis.get(JIVE);

	}

	@Test
	public void get_expired_key_should_return_null() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 1, TimeUnit.SECONDS);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.get(JIVE), is(nullValue()));

	}

	@Test
	public void get_none_expired_key_should_return_value() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 5, TimeUnit.SECONDS);

		assertThat(embeddedJedis.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void exists_should_return_true_if_element_exists() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.exists(JIVE), is(true));

	}

	@Test
	public void exists_should_return_false_if_element_non_exist() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.exists(WALTZ), is(false));

	}

	@Test
	public void exists_should_return_false_if_element_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.set(WALTZ, DARK_WALTZ);
		embeddedJedis.expire(WALTZ, 1);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.exists(WALTZ), is(false));

	}

	@Test
	public void type_should_return_type_of_element() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		assertThat(embeddedJedis.type(JIVE), is(StringDatatypeOperations.STRING));

	}

	@Test
	public void type_should_return_none_if_key_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.type(JIVE), is(KeysServerOperations.NONE));

	}

	@Test
	public void expire_should_add_expire_time() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		Long result = embeddedJedis.expire(JIVE, 1);
		assertThat(result, is(1L));

	}

	@Test
	public void expire_should_return_0_if_key_not_present() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		Long result = embeddedJedis.expire(WALTZ, 1);
		assertThat(result, is(0L));

	}

	@Test
	public void expire_at_should_add_expire_time() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		Long result = embeddedJedis.expireAt(JIVE,
				TimeUnit.SECONDS.convert(System.currentTimeMillis() + 5000, TimeUnit.MILLISECONDS));
		assertThat(result, is(1L));

		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));

	}

	@Test
	public void ttl_should_return_time_to_live_for_an_expiration_key() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 5);
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(embeddedJedis.ttl(JIVE), is(4L));

	}

	@Test
	public void ttl_should_return_a_negative_time_to_live_for_an_expired_key() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.ttl(JIVE), is(-1L));

	}

	@Test
	public void ttl_should_return_a_negative_time_to_live_for_not_managed_key() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(WALTZ, 1);
		assertThat(embeddedJedis.ttl(JIVE), is(-1L));

	}

	@Test
	public void getset_should_return_previous_value_and_set_new_value() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		byte[] previousElement = embeddedJedis.getSet(JIVE, ROCK_THIS_TOWN);

		assertThat(previousElement, is(MY_JUANITA));
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(ROCK_THIS_TOWN));

	}

	@Test
	public void getset_should_return_null_value_if_key_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);

		byte[] previousElement = embeddedJedis.getSet(JIVE, ROCK_THIS_TOWN);

		assertThat(previousElement, is(nullValue()));
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(ROCK_THIS_TOWN));

	}

	@Test(expected = IllegalArgumentException.class)
	public void getset_should_throw_an_exception_if_key_is_not_string_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);
		embeddedJedis.getSet(JIVE, ROCK_THIS_TOWN);

	}

	@Test
	public void setnx_should_add_element_only_if_not_exist() {

		Long result = embeddedJedis.setnx(JIVE, MY_JUANITA);
		assertThat(result, is(1L));
		assertThat(embeddedJedis.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void setnx_should_not_add_element_if_previous_key_present() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		Long result = embeddedJedis.setnx(JIVE, ROCK_THIS_TOWN);
		assertThat(result, is(0L));
		assertThat(embeddedJedis.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void setnx_should_add_element_if_previous_key_is_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);

		Long result = embeddedJedis.setnx(JIVE, ROCK_THIS_TOWN);
		assertThat(result, is(1L));
		assertThat(embeddedJedis.get(JIVE), is(ROCK_THIS_TOWN));

	}

	@Test
	public void setex_should_add_element_with_expiration() {

		String result = embeddedJedis.setex(JIVE, 2, MY_JUANITA);
		assertThat(result, is("OK"));
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));
		assertThat(embeddedJedis.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void setex_should_overwrite_ttl_time() throws InterruptedException {

		embeddedJedis.setex(JIVE, 2, MY_JUANITA);
		TimeUnit.SECONDS.sleep(2);
		embeddedJedis.setex(JIVE, 10, MY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));
	}

	@Test
	public void incr_should_increment_value() {

		embeddedJedis.set(JIVE, DURATION);
		Long newValue = embeddedJedis.incr(JIVE);
		assertThat(newValue, is(13L));

	}

	@Test
	public void incr_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(JIVE, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.incr(JIVE);
		assertThat(newValue, is(1L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void incr_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incr(JIVE);

	}

	@Test
	public void incrBy_should_increment_value() {

		embeddedJedis.set(JIVE, DURATION);
		Long newValue = embeddedJedis.incrBy(JIVE, 2);
		assertThat(newValue, is(14L));

	}

	@Test
	public void incrBy_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(JIVE, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.incrBy(JIVE, 2);
		assertThat(newValue, is(2L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void incrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incrBy(JIVE, 2);

	}

	@Test
	public void decr_should_increment_value() {

		embeddedJedis.set(JIVE, DURATION);
		Long newValue = embeddedJedis.decr(JIVE);
		assertThat(newValue, is(11L));

	}

	@Test
	public void decr_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(JIVE, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.decr(JIVE);
		assertThat(newValue, is(-1L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void decr_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incr(JIVE);

	}

	@Test
	public void decrBy_should_increment_value() {

		embeddedJedis.set(JIVE, DURATION);
		Long newValue = embeddedJedis.decrBy(JIVE, 2);
		assertThat(newValue, is(10L));

	}

	@Test
	public void decrBy_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(JIVE, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.decrBy(JIVE, 2);
		assertThat(newValue, is(-2L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void decrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incrBy(JIVE, 2);

	}

	@Test
	public void append_should_append_value() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.append(JIVE, ROCK_THIS_TOWN);
		byte[] concatSongs = embeddedJedis.get(JIVE);
		assertThat(concatSongs, is(CONCAT_JIVE));

	}

	@Test
	public void dbsize_should_return_zero_if_keys_expired_value() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.set(WALTZ, DARK_WALTZ);
		
		assertThat(embeddedJedis.dbSize(), is(2L));
		
		embeddedJedis.expire(JIVE, 1);
		embeddedJedis.expire(WALTZ, 1);
		
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		assertThat(embeddedJedis.dbSize(), is(0L));
	}
	
	@Test
	public void rename_should_return_ko_if_key_expired_value() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		
		embeddedJedis.expire(JIVE, 1);
		
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		assertThat(embeddedJedis.rename(JIVE, WALTZ), is("-"));
	}
	
	@Test
	public void renamenx_should_rename_key_if_new_key_expired_value() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.set(WALTZ, DARK_WALTZ);
		
		assertThat(embeddedJedis.renamenx(JIVE, WALTZ), is(0L));
		
		embeddedJedis.expire(WALTZ, 1);
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		
		assertThat(embeddedJedis.renamenx(JIVE, WALTZ), is(1L));
	}
	
	@Test
	public void keys_should_not_return_expired_keys() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.set(WALTZ, DARK_WALTZ);
		
		Set<byte[]> allKeys = embeddedJedis.keys("*".getBytes());
		assertThat(allKeys.size(), is(2));
		
		embeddedJedis.expire(WALTZ, 1);
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		
		allKeys = embeddedJedis.keys("*".getBytes());
		assertThat(allKeys.size(), is(1));
	}
	
	@Test
	public void persist_should_avoid_expiration() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		embeddedJedis.persist(JIVE);
		TimeUnit.SECONDS.sleep(2);
		byte[] newSong = embeddedJedis.get(JIVE);
		assertThat(newSong, is(MY_JUANITA));

	}
	
	@Test
	public void append_should_insertvalue_if_expired_key() throws InterruptedException {

		embeddedJedis.set(JIVE, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		embeddedJedis.append(JIVE, ROCK_THIS_TOWN);
		byte[] newSong = embeddedJedis.get(JIVE);
		assertThat(newSong, is(ROCK_THIS_TOWN));

	}

	@Test(expected = IllegalArgumentException.class)
	public void append_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.append(JIVE, MY_JUANITA);

	}

	@Test
	public void substr_should_return_subtring() {

		embeddedJedis.set(JIVE, MY_JUANITA);
		byte[] subSong = embeddedJedis.substr(JIVE, 0, MY_JUANITA.length - 1);
		assertThat(subSong, is(MY_JUANITA));

	}

	@Test
	public void substr_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		byte[] subSong = embeddedJedis.substr(JIVE, 0, MY_JUANITA.length - 1);
		assertThat(subSong, is(new byte[0]));

	}

	@Test
	public void hset_should_set_data() {

		Long result = embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		assertThat(result, is(1L));
		byte[] title = embeddedJedis.hget(JIVE, SONG);
		assertThat(title, is(MY_JUANITA));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hset_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);

	}

	@Test
	public void hget_should_get_inserted_data() {

		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		byte[] title = embeddedJedis.hget(JIVE, SONG);
		assertThat(title, is(MY_JUANITA));

	}

	@Test
	public void hget_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		byte[] title = embeddedJedis.hget(JIVE, SONG);
		assertThat(title, is(nullValue()));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hget_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, SONG);
		embeddedJedis.hget(JIVE, SONG);

	}

	@Test
	public void hsetnx_should_set_data_if_not_exist() {

		Long result = embeddedJedis.hsetnx(JIVE, SONG, MY_JUANITA);
		assertThat(result, is(1L));
		byte[] title = embeddedJedis.hget(JIVE, SONG);
		assertThat(title, is(MY_JUANITA));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hsetnx_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hsetnx(JIVE, SONG, MY_JUANITA);

	}

	@Test
	public void hsetnx_should_return_new_set_field_if_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long result = embeddedJedis.hsetnx(JIVE, SONG, ROCK_THIS_TOWN);
		byte[] title = embeddedJedis.hget(JIVE, SONG);
		assertThat(title, is(ROCK_THIS_TOWN));
		assertThat(result, is(1L));

	}

	@Test
	public void hincrBy_should_increment_value() {
		embeddedJedis.hset(JIVE, SONG, DURATION);
		Long result = embeddedJedis.hincrBy(JIVE, SONG, 2);
		assertThat(result, is(14L));
	}

	@Test
	public void hincrBy_should_set_value_if_expired_key() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long result = embeddedJedis.hincrBy(JIVE, SONG, 2);
		assertThat(result, is(2L));
	}

	@Test(expected = IllegalArgumentException.class)
	public void hincrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hincrBy(JIVE, SONG, 2);
	}

	@Test
	public void hmset_should_add_all_fields() {

		Map<byte[], byte[]> fields = new HashMap<byte[], byte[]>();
		fields.put(SONG, MY_JUANITA);
		embeddedJedis.hmset(JIVE, fields);

	}

	@Test(expected = IllegalArgumentException.class)
	public void hmset_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		Map<byte[], byte[]> fields = new HashMap<byte[], byte[]>();
		fields.put(SONG, MY_JUANITA);
		String result = embeddedJedis.hmset(JIVE, fields);
		assertThat(result, is("OK"));
	}

	@Test
	public void hmget_should_add_all_fields() {

		Map<byte[], byte[]> fields = new HashMap<byte[], byte[]>();
		fields.put(SONG, MY_JUANITA);
		embeddedJedis.hmset(JIVE, fields);

		List<byte[]> songs = embeddedJedis.hmget(JIVE, SONG);
		assertThat(songs, contains(MY_JUANITA));

	}

	@Test
	public void hmget_should_return_list_of_null_if_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		List<byte[]> songs = embeddedJedis.hmget(JIVE, SONG);
		assertThat(songs.get(0), nullValue());
	}

	@Test(expected = IllegalArgumentException.class)
	public void hmget_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		Map<byte[], byte[]> fields = new HashMap<byte[], byte[]>();
		fields.put(SONG, MY_JUANITA);
		embeddedJedis.hmset(JIVE, fields);

	}

	@Test
	public void hexists_should_return_false_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		boolean isKeyPresent = embeddedJedis.hexists(JIVE, SONG);

		assertThat(isKeyPresent, is(false));
	}

	@Test
	public void hdel_should_return_zero_of_removed_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.expire(JIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long numberOfRemovedElements = embeddedJedis.hdel(JIVE, SONG);

		assertThat(numberOfRemovedElements, is(0L));

	}

	@Test
	public void hlen_should_return_zero_if_key_is_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.hset(JIVE, WALTZ, DARK_WALTZ);
		embeddedJedis.expire(JIVE, 1);
		Long lengthOfElement = embeddedJedis.hlen(JIVE);
		assertThat(lengthOfElement, is(2L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		lengthOfElement = embeddedJedis.hlen(JIVE);

		assertThat(lengthOfElement, is(0L));

	}

	@Test
	public void hkeys_should_return_no_keys_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, DURATION);
		embeddedJedis.expire(JIVE, 1);
		Set<byte[]> numberOfKeys = embeddedJedis.hkeys(JIVE);
		assertThat(numberOfKeys, hasSize(1));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		numberOfKeys = embeddedJedis.hkeys(JIVE);

		assertThat(numberOfKeys, hasSize(0));

	}

	@Test
	public void hvals_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		embeddedJedis.hset(JIVE, WALTZ, DARK_WALTZ);
		embeddedJedis.expire(JIVE, 1);
		Collection<byte[]> elements = embeddedJedis.hvals(JIVE);
		assertThat(elements, hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		elements = embeddedJedis.hvals(JIVE);

		assertThat(elements, hasSize(0));

	}

	@Test
	public void hgetAll_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(JIVE, SONG, MY_JUANITA);
		embeddedJedis.hset(JIVE, WALTZ, DARK_WALTZ);
		embeddedJedis.expire(JIVE, 1);
		Map<byte[], byte[]> elements = embeddedJedis.hgetAll(JIVE);
		assertThat(elements.entrySet(), hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		elements = embeddedJedis.hgetAll(JIVE);

		assertThat(elements.entrySet(), hasSize(0));

	}

	@Test
	public void rpush_should_add_new_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.rpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.rpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		Long numberOfTotalInsertedElements = embeddedJedis.rpush(JIVE, MY_JUANITA);
		assertThat(numberOfTotalInsertedElements, is(3L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		Long result = embeddedJedis.rpush(JIVE, MY_JUANITA);
		assertThat(result, is(1L));

	}

	@Test
	public void lpush_should_add_new_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		Long numberOfTotalInsertedElements = embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		assertThat(numberOfTotalInsertedElements, is(3L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		Long result = embeddedJedis.lpush(JIVE, MY_JUANITA);
		assertThat(result, is(1L));

	}

	@Test
	public void llen_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		assertThat(embeddedJedis.llen(JIVE), is(0L));

	}

	@Test
	public void lrange_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		List<byte[]> elements = embeddedJedis.lrange(JIVE, 0, -1);
		assertThat(elements, hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.lrange(JIVE, 0, -1);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void ltrim_should_return_ko_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		embeddedJedis.ltrim(JIVE, 1, -1);
		List<byte[]> elements = embeddedJedis.lrange(JIVE, 0, -1);
		assertThat(elements, hasSize(1));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		String result = embeddedJedis.ltrim(JIVE, 1, -1);
		assertThat(result, is("-"));

	}

	@Test
	public void lindex_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		embeddedJedis.expire(JIVE, 1);
		byte[] element = embeddedJedis.lindex(JIVE, 0);
		assertThat(element, is(MY_JUANITA));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.lindex(JIVE, 0);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void lset_should_return_ko_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		String result = embeddedJedis.lset(JIVE, 0, DARK_WALTZ);
		assertThat(result, is("OK"));
		List<byte[]> elements = embeddedJedis.lrange(JIVE, 0, -1);
		assertThat(elements, hasSize(2));
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		result = embeddedJedis.lset(JIVE, 0, MY_JUANITA);
		assertThat(result, is("-"));

	}

	@Test
	public void lrem_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		String result = embeddedJedis.lset(JIVE, 0, DARK_WALTZ);
		assertThat(result, is("OK"));
		Long removed = embeddedJedis.lrem(JIVE, 0, ROCK_THIS_TOWN);
		assertThat(removed, is(1L));
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removed = embeddedJedis.lrem(JIVE, 0, MY_JUANITA);
		assertThat(removed, is(0L));

	}

	@Test
	public void lpop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		byte[] element = embeddedJedis.lpop(JIVE);
		assertThat(element, is(MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.lpop(JIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void rpop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		byte[] element = embeddedJedis.rpop(JIVE);
		assertThat(element, is(ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.rpop(JIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void sadd_should_add_new_element_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		Long numberOfElements = embeddedJedis.setDatatypeOperations.scard(JIVE);
		assertThat(numberOfElements, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		embeddedJedis.sadd(JIVE, MY_JUANITA);
		numberOfElements = embeddedJedis.setDatatypeOperations.scard(JIVE);
		assertThat(numberOfElements, is(1L));

	}

	@Test
	public void smembers_should_return_empty_list_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		Set<byte[]> elements = embeddedJedis.smembers(JIVE);

		assertThat(elements, contains(ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.smembers(JIVE);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void srem_should_remove_no_element_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.sadd(JIVE, MY_JUANITA);
		Long removedElements = embeddedJedis.srem(JIVE, MY_JUANITA);

		assertThat(removedElements, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.srem(JIVE, ROCK_THIS_TOWN);
		assertThat(removedElements, is(0L));

	}

	@Test
	public void spop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		byte[] removedElements = embeddedJedis.spop(JIVE);

		assertThat(removedElements, is(ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.spop(JIVE);
		assertThat(removedElements, is(nullValue()));

	}

	@Test
	public void scard_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.sadd(JIVE, MY_JUANITA);
		Long numberOfElements = embeddedJedis.scard(JIVE);

		assertThat(numberOfElements, is(2L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.scard(JIVE);
		assertThat(numberOfElements, is(0L));

	}

	@Test
	public void sismember_should_return_false_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.sadd(JIVE, MY_JUANITA);
		boolean present = embeddedJedis.sismember(JIVE, ROCK_THIS_TOWN);

		assertThat(present, is(true));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		present = embeddedJedis.sismember(JIVE, ROCK_THIS_TOWN);

		assertThat(present, is(false));

	}

	@Test
	public void srandmember_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.sadd(JIVE, MY_JUANITA);
		byte[] element = embeddedJedis.srandmember(JIVE);

		assertThat(element, is(not(nullValue())));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.srandmember(JIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void zadd_should_add_new_element_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		assertThat(embeddedJedis.sortsetDatatypeOperations.zcard(JIVE), is(1L));

	}

	@Test
	public void zadd_should_add_new_map_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		Map<Double, byte[]> elements = new HashMap<Double, byte[]>();
		elements.put(1D, MY_JUANITA);

		embeddedJedis.zadd(JIVE, elements);
		assertThat(embeddedJedis.sortsetDatatypeOperations.zcard(JIVE), is(1L));

	}

	@Test
	public void zrem_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrange(JIVE, 0, -1);
		assertThat(elements, contains(MY_JUANITA, ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrange(JIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrange_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrange(JIVE, 0, -1);
		assertThat(elements, contains(MY_JUANITA, ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrange(JIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zincrby_should_set_value_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, DURATION);

		double newValue = embeddedJedis.zincrby(JIVE, 2D, DURATION);
		assertThat(newValue, is(3D));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		newValue = embeddedJedis.zincrby(JIVE, 2D, DURATION);
		assertThat(newValue, is(2D));

	}

	@Test
	public void zrank_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);

		Long indexValue = embeddedJedis.zrank(JIVE, MY_JUANITA);
		assertThat(indexValue, is(0L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		indexValue = embeddedJedis.zrank(JIVE, MY_JUANITA);
		assertThat(indexValue, is(nullValue()));

	}

	@Test
	public void zrevrank_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);

		Long indexValue = embeddedJedis.zrank(JIVE, MY_JUANITA);
		assertThat(indexValue, is(0L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		indexValue = embeddedJedis.zrevrank(JIVE, MY_JUANITA);
		assertThat(indexValue, is(nullValue()));

	}

	@Test
	public void zrevrange_should_remove_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrevrange(JIVE, 0, -1);
		assertThat(elements, contains(ROCK_THIS_TOWN, MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrange(JIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrangeWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeWithScores(JIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeWithScores(JIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void zrevrangeWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeWithScores(JIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeWithScores(JIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void zcard_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long numberOfElements = embeddedJedis.zcard(JIVE);
		assertThat(numberOfElements, is(2L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.zcard(JIVE);
		assertThat(numberOfElements, is(0L));

	}

	@Test
	public void zscore_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Double score = embeddedJedis.zscore(JIVE, ROCK_THIS_TOWN);
		assertThat(score, is(2D));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		score = embeddedJedis.zscore(JIVE, ROCK_THIS_TOWN);
		assertThat(score, is(nullValue()));

	}

	@Test
	public void sort_should_return_empty_list_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		List<byte[]> elements = embeddedJedis.sort(JIVE);
		assertThat(elements, contains(MY_JUANITA, ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.sort(JIVE);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zcount_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long numberOfElementsBetweenRange = embeddedJedis.zcount(JIVE, 0, 1);
		assertThat(numberOfElementsBetweenRange, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElementsBetweenRange = embeddedJedis.zcount(JIVE, 0, 1);
		assertThat(numberOfElementsBetweenRange, is(0L));

	}

	@Test
	public void zcount_with_infinite_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long numberOfElementsBetweenRange = embeddedJedis.zcount(JIVE, "-inf".getBytes(), "+inf".getBytes());
		assertThat(numberOfElementsBetweenRange, is(2L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElementsBetweenRange = embeddedJedis.zcount(JIVE, "-inf".getBytes(), "+inf".getBytes());
		assertThat(numberOfElementsBetweenRange, is(0L));

	}

	@Test
	public void zrangeByScore_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrangeByScore(JIVE, 0, 1);
		assertThat(elements, contains(MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(JIVE, 0, 1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrangeByScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrangeByScore(JIVE, "-inf".getBytes(), "1".getBytes());
		assertThat(elements, contains(MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(JIVE, "-inf".getBytes(), "+inf".getBytes());
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScore_with_infinite_and_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrangeByScore(JIVE, "-inf".getBytes(), "1".getBytes(), 0, 1);
		assertThat(elements, contains(MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(JIVE, "-inf".getBytes(), "+inf".getBytes(), 0, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrangeByScore(JIVE, 0, 2, 1, 1);
		assertThat(elements, contains(ROCK_THIS_TOWN));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(JIVE, 0, 2, 1, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(JIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(JIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_with_infinite_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(JIVE, "-inf".getBytes(), "+inf".getBytes());
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(JIVE, "-inf".getBytes(), "+inf".getBytes());
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_with_infinite_and_offset_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(JIVE, "-inf".getBytes(), "+inf".getBytes(), 1, 1);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(JIVE, "-inf".getBytes(), "+inf".getBytes(), 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScore_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrevrangeByScore(JIVE, 2, 0);
		assertThat(elements, contains(ROCK_THIS_TOWN, MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(JIVE, 0, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrevrangeByScore(JIVE, 2, 0, 1, 1);
		assertThat(elements, contains(MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(JIVE, 2, 0, 1, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<byte[]> elements = embeddedJedis.zrevrangeByScore(JIVE, "+inf".getBytes(), "-inf".getBytes());
		assertThat(elements, contains(ROCK_THIS_TOWN, MY_JUANITA));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(JIVE, "+inf".getBytes(), "-inf".getBytes());
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScoreWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, 3, 0);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, 3, 0);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, 3, 0, 1, 1);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		assertThat(elements, contains(myJuanitaTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, 3, 0, 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, "+inf".getBytes(), "-inf".getBytes());
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, "+inf".getBytes(), "-inf".getBytes());
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_infinite_and_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, "+inf".getBytes(), "-inf".getBytes(), 1, 1);
		Tuple myJuanitaTuple = new Tuple(MY_JUANITA, 1D);
		assertThat(elements, contains(myJuanitaTuple));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(JIVE, "+inf".getBytes(), "-inf".getBytes(), 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zremrangeByRank_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByRank(JIVE, 0, 0);
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByRank(JIVE, 0, 1);
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void zremrangeByScore_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByScore(JIVE, 0, 1);
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByScore(JIVE, 0, 1);
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void zremrangeByScore__with_infinite_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(JIVE, 1, MY_JUANITA);
		embeddedJedis.zadd(JIVE, 2, ROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByScore(JIVE, "-inf".getBytes(), "1".getBytes());
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByScore(JIVE, "-inf".getBytes(), "1".getBytes());
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void linsert_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		
		Long numberOfElements = embeddedJedis.linsert(JIVE, LIST_POSITION.AFTER, MY_JUANITA, DARK_WALTZ);
		assertThat(numberOfElements, is(3L));
			
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.linsert(JIVE, LIST_POSITION.AFTER, MY_JUANITA, DARK_WALTZ);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void lpushx_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		
		Long numberOfElements = embeddedJedis.lpushx(JIVE, MY_JUANITA);
		assertThat(numberOfElements, is(3L));
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.lpushx(JIVE, MY_JUANITA);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void rpushx_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(JIVE, ROCK_THIS_TOWN);
		embeddedJedis.lpush(JIVE, MY_JUANITA);
		
		Long numberOfElements = embeddedJedis.rpushx(JIVE, MY_JUANITA);
		assertThat(numberOfElements, is(3L));
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.rpushx(JIVE, MY_JUANITA);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void setbit_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.set(JIVE, ROCK_THIS_TOWN);
		
		embeddedJedis.setbit(JIVE, 2, "1".getBytes());
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		boolean previousBit = embeddedJedis.setbit(JIVE, 2, "1".getBytes());
		assertThat(previousBit, is(false));

	}
	
	@Test
	public void getbit_should_return_zero_if_key_expired() throws InterruptedException {

		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		embeddedJedis.stringDatatypeOperations.simpleTypes.put(wrap(JIVE), wrap(values));
		assertThat(embeddedJedis.getbit(JIVE, 8), is(true));
		
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		assertThat(embeddedJedis.getbit(JIVE, 8), is(false));

	}
	
	@Test
	public void setrange_should_add_as_new_if_key_expired() throws InterruptedException {

		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		embeddedJedis.stringDatatypeOperations.simpleTypes.put(wrap(JIVE), wrap(values));
		Long newLength = embeddedJedis.setrange(JIVE, 0, values);
		
		assertThat(newLength, is(2L));
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		newLength = embeddedJedis.setrange(JIVE, 0, values);
		assertThat(newLength, is(2L));

	}
	
	@Test
	public void getrange_should_add_as_new_if_key_expired() throws InterruptedException {

		embeddedJedis.stringDatatypeOperations.append(JIVE, MY_JUANITA);
		
		byte[] result = embeddedJedis.getrange(JIVE, 1, 3);
		assertThat(result.length, is(3));
		
		embeddedJedis.expire(JIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		result = embeddedJedis.getrange(JIVE, 1, 3);
		assertThat(result.length, is(0));

	}
	
}
