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

import redis.clients.jedis.Tuple;
import redis.clients.jedis.BinaryClient.LIST_POSITION;

import com.lordofthejars.nosqlunit.redis.embedded.ExpirationDatatypeOperations.TtlState;

public class EmbeddedJedisUsedWithString {

	private static final int SLEEP_IN_MILLIS = 1200;
	private static final String SJIVE = "Jive";
	private static final byte[] JIVE = SJIVE.getBytes();
	private static final String SMY_JUANITA = "My Juanita";
	private static final byte[] MY_JUANITA = SMY_JUANITA.getBytes();
	private static final String SROCK_THIS_TOWN = "Rock This Town";
	private static final byte[] ROCK_THIS_TOWN = SROCK_THIS_TOWN.getBytes();
	private static final String SCONCAT_JIVE = "My JuanitaRock This Town";
	private static final String SSONG = "Song";
	private static final byte[] SONG = SSONG.getBytes();

	private static final String SWALTZ = "Waltz";
	private static final String SDARK_WALTZ = "Dark Waltz";

	private static final String SDURATION = "12";
	private static final byte[] DURATION = SDURATION.getBytes();

	private EmbeddedJedis embeddedJedis;

	@Before
	public void setUp() {
		embeddedJedis = new EmbeddedJedis();
	}

	@Test
	public void del_should_remove_all_keys() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.del(SJIVE);
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(nullValue()));

	}
	
	@Test
	public void set_should_set_string_value() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(MY_JUANITA));

	}

	@Test
	public void set_should_remove_any_previous_key() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(MY_JUANITA));
		assertThat(embeddedJedis.listDatatypeOperations.llen(JIVE), is(0L));

	}

	@Test
	public void set_should_remove_expiration_time() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 10, TimeUnit.SECONDS);

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_MANAGED));

	}

	@Test
	public void get_should_get_inserted_string_element() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.get(SJIVE), is(SMY_JUANITA));

	}

	@Test
	public void get_should_return_null_if_key_not_present() {
		assertThat(embeddedJedis.get(SJIVE), is(nullValue()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void get_should_throw_an_exception_if_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);
		embeddedJedis.get(SJIVE);

	}

	@Test
	public void get_expired_key_should_return_null() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 1, TimeUnit.SECONDS);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.get(SJIVE), is(nullValue()));

	}

	@Test
	public void get_none_expired_key_should_return_value() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.stringDatatypeOperations.addExpirationTime(JIVE, 5, TimeUnit.SECONDS);

		assertThat(embeddedJedis.get(SJIVE), is(SMY_JUANITA));

	}

	@Test
	public void persist_should_avoid_expiration() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		embeddedJedis.persist(SJIVE);
		TimeUnit.SECONDS.sleep(2);
		String newSong = embeddedJedis.get(SJIVE);
		assertThat(newSong, is(SMY_JUANITA));

	}
	
	@Test
	public void keys_should_not_return_expired_keys() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.set(SWALTZ, SDARK_WALTZ);
		
		Set<String> allKeys = embeddedJedis.keys("*");
		assertThat(allKeys.size(), is(2));
		
		embeddedJedis.expire(SWALTZ, 1);
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		
		allKeys = embeddedJedis.keys("*");
		assertThat(allKeys.size(), is(1));
	}
	
	@Test
	public void renamenx_should_rename_key_if_new_key_expired_value() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.set(SWALTZ, SDARK_WALTZ);
		
		assertThat(embeddedJedis.renamenx(SJIVE, SWALTZ), is(0L));
		
		embeddedJedis.expire(SWALTZ, 1);
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		
		assertThat(embeddedJedis.renamenx(SJIVE, SWALTZ), is(1L));
	}
	
	@Test
	public void rename_should_return_ko_if_key_expired_value() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		
		embeddedJedis.expire(SJIVE, 1);
		
		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		assertThat(embeddedJedis.rename(SJIVE, SWALTZ), is("-"));
	}
	
	@Test
	public void exists_should_return_true_if_element_exists() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.exists(SJIVE), is(true));

	}

	@Test
	public void exists_should_return_false_if_element_non_exist() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.exists(SWALTZ), is(false));

	}

	@Test
	public void exists_should_return_false_if_element_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.set(SWALTZ, SDARK_WALTZ);
		embeddedJedis.expire(SWALTZ, 1);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.exists(SWALTZ), is(false));

	}

	@Test
	public void type_should_return_type_of_element() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		assertThat(embeddedJedis.type(SJIVE), is(StringDatatypeOperations.STRING));

	}

	@Test
	public void type_should_return_none_if_key_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.type(SJIVE), is(KeysServerOperations.NONE));

	}

	@Test
	public void expire_should_add_expire_time() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		Long result = embeddedJedis.expire(SJIVE, 1);
		assertThat(result, is(1L));

	}

	@Test
	public void expire_should_return_0_if_key_not_present() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		Long result = embeddedJedis.expire(SWALTZ, 1);
		assertThat(result, is(0L));

	}

	@Test
	public void expire_at_should_add_expire_time() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		Long result = embeddedJedis.expireAt(SJIVE,
				TimeUnit.SECONDS.convert(System.currentTimeMillis() + 5000, TimeUnit.MILLISECONDS));
		assertThat(result, is(1L));

		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));

	}

	@Test
	public void ttl_should_return_time_to_live_for_an_expiration_key() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 5);
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(embeddedJedis.ttl(SJIVE), is(4L));

	}

	@Test
	public void ttl_should_return_a_negative_time_to_live_for_an_expired_key() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		assertThat(embeddedJedis.ttl(SJIVE), is(-1L));

	}

	@Test
	public void ttl_should_return_a_negative_time_to_live_for_not_managed_key() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SWALTZ, 1);
		assertThat(embeddedJedis.ttl(SJIVE), is(-1L));

	}

	@Test
	public void getset_should_return_previous_value_and_set_new_value() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		String previousElement = embeddedJedis.getSet(SJIVE, SROCK_THIS_TOWN);

		assertThat(previousElement, is(SMY_JUANITA));
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(ROCK_THIS_TOWN));

	}

	@Test
	public void getset_should_return_null_value_if_key_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);

		String previousElement = embeddedJedis.getSet(SJIVE, SROCK_THIS_TOWN);

		assertThat(previousElement, is(nullValue()));
		assertThat(embeddedJedis.stringDatatypeOperations.get(JIVE), is(ROCK_THIS_TOWN));

	}

	@Test(expected = IllegalArgumentException.class)
	public void getset_should_throw_an_exception_if_key_is_not_string_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, MY_JUANITA);
		embeddedJedis.getSet(SJIVE, SROCK_THIS_TOWN);

	}

	@Test
	public void setnx_should_add_element_only_if_not_exist() {

		Long result = embeddedJedis.setnx(SJIVE, SMY_JUANITA);
		assertThat(result, is(1L));
		assertThat(embeddedJedis.get(SJIVE), is(SMY_JUANITA));

	}

	@Test
	public void setnx_should_not_add_element_if_previous_key_present() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		Long result = embeddedJedis.setnx(SJIVE, SROCK_THIS_TOWN);
		assertThat(result, is(0L));
		assertThat(embeddedJedis.get(SJIVE), is(SMY_JUANITA));

	}

	@Test
	public void setnx_should_add_element_if_previous_key_is_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);

		Long result = embeddedJedis.setnx(SJIVE, SROCK_THIS_TOWN);
		assertThat(result, is(1L));
		assertThat(embeddedJedis.get(SJIVE), is(SROCK_THIS_TOWN));

	}

	@Test
	public void setex_should_add_element_with_expiration() {

		String result = embeddedJedis.setex(SJIVE, 2, SMY_JUANITA);
		assertThat(result, is("OK"));
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));
		assertThat(embeddedJedis.get(SJIVE), is(SMY_JUANITA));

	}

	@Test
	public void setex_should_overwrite_ttl_time() throws InterruptedException {

		embeddedJedis.setex(SJIVE, 2, SMY_JUANITA);
		TimeUnit.SECONDS.sleep(2);
		embeddedJedis.setex(SJIVE, 10, SMY_JUANITA);
		assertThat(embeddedJedis.stringDatatypeOperations.timedoutState(JIVE), is(TtlState.NOT_EXPIRED));
	}

	@Test
	public void incr_should_increment_value() {

		embeddedJedis.set(SJIVE, SDURATION);
		Long newValue = embeddedJedis.incr(SJIVE);
		assertThat(newValue, is(13L));

	}

	@Test
	public void incr_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(SJIVE, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.incr(SJIVE);
		assertThat(newValue, is(1L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void incr_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incr(SJIVE);

	}

	@Test
	public void incrBy_should_increment_value() {

		embeddedJedis.set(SJIVE, SDURATION);
		Long newValue = embeddedJedis.incrBy(SJIVE, 2);
		assertThat(newValue, is(14L));

	}

	@Test
	public void incrBy_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(SJIVE, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.incrBy(SJIVE, 2);
		assertThat(newValue, is(2L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void incrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incrBy(SJIVE, 2);

	}

	@Test
	public void decr_should_increment_value() {

		embeddedJedis.set(SJIVE, SDURATION);
		Long newValue = embeddedJedis.decr(SJIVE);
		assertThat(newValue, is(11L));

	}

	@Test
	public void decr_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(SJIVE, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.decr(SJIVE);
		assertThat(newValue, is(-1L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void decr_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incr(SJIVE);

	}

	@Test
	public void decrBy_should_increment_value() {

		embeddedJedis.set(SJIVE, SDURATION);
		Long newValue = embeddedJedis.decrBy(SJIVE, 2);
		assertThat(newValue, is(10L));

	}

	@Test
	public void decrBy_should_reset_and_increment_expired_keys() throws InterruptedException {

		embeddedJedis.set(SJIVE, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long newValue = embeddedJedis.decrBy(SJIVE, 2);
		assertThat(newValue, is(-2L));

	}

	@Test(expected = IllegalArgumentException.class)
	public void decrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.incrBy(SJIVE, 2);

	}

	@Test
	public void append_should_append_value() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.append(SJIVE, SROCK_THIS_TOWN);
		String concatSongs = embeddedJedis.get(SJIVE);
		assertThat(concatSongs, is(SCONCAT_JIVE));

	}

	@Test
	public void append_should_insertvalue_if_expired_key() throws InterruptedException {

		embeddedJedis.set(SJIVE, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		embeddedJedis.append(SJIVE, SROCK_THIS_TOWN);
		String newSong = embeddedJedis.get(SJIVE);
		assertThat(newSong, is(SROCK_THIS_TOWN));

	}

	@Test(expected = IllegalArgumentException.class)
	public void append_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.append(SJIVE, SMY_JUANITA);

	}

	@Test
	public void substr_should_return_subtring() {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		String subSong = embeddedJedis.substr(SJIVE, 0, SMY_JUANITA.length() - 1);
		assertThat(subSong, is(SMY_JUANITA));

	}

	@Test
	public void substr_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		String subSong = embeddedJedis.substr(SJIVE, 0, SMY_JUANITA.length() - 1);
		assertThat(subSong, is(""));

	}

	@Test
	public void hset_should_set_data() {

		Long result = embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		assertThat(result, is(1L));
		String title = embeddedJedis.hget(SJIVE, SSONG);
		assertThat(title, is(SMY_JUANITA));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hset_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);

	}

	@Test
	public void hget_should_get_inserted_data() {

		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		String title = embeddedJedis.hget(SJIVE, SSONG);
		assertThat(title, is(SMY_JUANITA));

	}

	@Test
	public void hget_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		String title = embeddedJedis.hget(SJIVE, SSONG);
		assertThat(title, is(nullValue()));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hget_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, SONG);
		embeddedJedis.hget(SJIVE, SSONG);

	}

	@Test
	public void hsetnx_should_set_data_if_not_exist() {

		Long result = embeddedJedis.hsetnx(SJIVE, SSONG, SMY_JUANITA);
		assertThat(result, is(1L));
		String title = embeddedJedis.hget(SJIVE, SSONG);
		assertThat(title, is(SMY_JUANITA));

	}

	@Test(expected = IllegalArgumentException.class)
	public void hsetnx_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hsetnx(SJIVE, SSONG, SMY_JUANITA);

	}

	@Test
	public void hsetnx_should_return_new_set_field_if_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long result = embeddedJedis.hsetnx(SJIVE, SSONG, SROCK_THIS_TOWN);
		String title = embeddedJedis.hget(SJIVE, SSONG);
		assertThat(title, is(SROCK_THIS_TOWN));
		assertThat(result, is(1L));

	}

	@Test
	public void hincrBy_should_increment_value() {
		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		Long result = embeddedJedis.hincrBy(SJIVE, SSONG, 2);
		assertThat(result, is(14L));
	}

	@Test
	public void hincrBy_should_set_value_if_expired_key() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long result = embeddedJedis.hincrBy(SJIVE, SSONG, 2);
		assertThat(result, is(2L));
	}

	@Test(expected = IllegalArgumentException.class)
	public void hincrBy_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		embeddedJedis.hincrBy(SJIVE, SSONG, 2);
	}

	@Test
	public void hmset_should_add_all_fields() {

		Map<String, String> fields = new HashMap<String, String>();
		fields.put(SSONG, SMY_JUANITA);
		embeddedJedis.hmset(SJIVE, fields);

	}

	@Test(expected = IllegalArgumentException.class)
	public void hmset_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(SSONG, SMY_JUANITA);
		String result = embeddedJedis.hmset(SJIVE, fields);
		assertThat(result, is("OK"));
	}

	@Test
	public void hmget_should_add_all_fields() {

		Map<String, String> fields = new HashMap<String, String>();
		fields.put(SSONG, SMY_JUANITA);
		embeddedJedis.hmset(SJIVE, fields);

		List<String> songs = embeddedJedis.hmget(SJIVE, SSONG);
		assertThat(songs, contains(SMY_JUANITA));

	}

	@Test
	public void hmget_should_return_list_of_null_if_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		List<String> songs = embeddedJedis.hmget(SJIVE, SSONG);
		assertThat(songs.get(0), nullValue());
	}

	@Test(expected = IllegalArgumentException.class)
	public void hmget_should_throw_an_exception_with_not_valid_type() {

		embeddedJedis.listDatatypeOperations.lpush(JIVE, DURATION);
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(SSONG, SMY_JUANITA);
		embeddedJedis.hmset(SJIVE, fields);

	}

	@Test
	public void hexists_should_return_false_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		boolean isKeyPresent = embeddedJedis.hexists(SJIVE, SSONG);

		assertThat(isKeyPresent, is(false));
	}

	@Test
	public void hdel_should_return_zero_of_removed_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		TimeUnit.SECONDS.sleep(2);
		Long numberOfRemovedElements = embeddedJedis.hdel(SJIVE, SSONG);

		assertThat(numberOfRemovedElements, is(0L));

	}

	@Test
	public void hlen_should_return_zero_if_key_is_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.hset(SJIVE, SWALTZ, SDARK_WALTZ);
		embeddedJedis.expire(SJIVE, 1);
		Long lengthOfElement = embeddedJedis.hlen(SJIVE);
		assertThat(lengthOfElement, is(2L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		lengthOfElement = embeddedJedis.hlen(SJIVE);

		assertThat(lengthOfElement, is(0L));

	}

	@Test
	public void hkeys_should_return_no_keys_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SDURATION);
		embeddedJedis.expire(SJIVE, 1);
		Set<String> numberOfKeys = embeddedJedis.hkeys(SJIVE);
		assertThat(numberOfKeys, hasSize(1));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		numberOfKeys = embeddedJedis.hkeys(SJIVE);

		assertThat(numberOfKeys, hasSize(0));

	}

	@Test
	public void hvals_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		embeddedJedis.hset(SJIVE, SWALTZ, SDARK_WALTZ);
		embeddedJedis.expire(SJIVE, 1);
		Collection<String> elements = embeddedJedis.hvals(SJIVE);
		assertThat(elements, hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		elements = embeddedJedis.hvals(SJIVE);

		assertThat(elements, hasSize(0));

	}

	@Test
	public void hgetAll_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.hset(SJIVE, SSONG, SMY_JUANITA);
		embeddedJedis.hset(SJIVE, SWALTZ, SDARK_WALTZ);
		embeddedJedis.expire(SJIVE, 1);
		Map<String, String> elements = embeddedJedis.hgetAll(SJIVE);
		assertThat(elements.entrySet(), hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		elements = embeddedJedis.hgetAll(SJIVE);

		assertThat(elements.entrySet(), hasSize(0));

	}

	@Test
	public void rpush_should_add_new_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.rpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.rpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		Long numberOfTotalInsertedElements = embeddedJedis.rpush(SJIVE, SMY_JUANITA);
		assertThat(numberOfTotalInsertedElements, is(3L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		Long result = embeddedJedis.rpush(SJIVE, SMY_JUANITA);
		assertThat(result, is(1L));

	}

	@Test
	public void lpush_should_add_new_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		Long numberOfTotalInsertedElements = embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		assertThat(numberOfTotalInsertedElements, is(3L));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		Long result = embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		assertThat(result, is(1L));

	}

	@Test
	public void llen_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);
		assertThat(embeddedJedis.llen(SJIVE), is(0L));

	}

	@Test
	public void lrange_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		List<String> elements = embeddedJedis.lrange(SJIVE, 0, -1);
		assertThat(elements, hasSize(2));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.lrange(SJIVE, 0, -1);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void ltrim_should_return_ko_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		embeddedJedis.ltrim(SJIVE, 1, -1);
		List<String> elements = embeddedJedis.lrange(SJIVE, 0, -1);
		assertThat(elements, hasSize(1));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		String result = embeddedJedis.ltrim(SJIVE, 1, -1);
		assertThat(result, is("-"));

	}

	@Test
	public void lindex_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		embeddedJedis.expire(SJIVE, 1);
		String element = embeddedJedis.lindex(SJIVE, 0);
		assertThat(element, is(SMY_JUANITA));

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.lindex(SJIVE, 0);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void lset_should_return_ko_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		String result = embeddedJedis.lset(SJIVE, 0, SDARK_WALTZ);
		assertThat(result, is("OK"));
		List<String> elements = embeddedJedis.lrange(SJIVE, 0, -1);
		assertThat(elements, hasSize(2));
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		result = embeddedJedis.lset(SJIVE, 0, SMY_JUANITA);
		assertThat(result, is("-"));

	}

	@Test
	public void lrem_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		String result = embeddedJedis.lset(SJIVE, 0, SDARK_WALTZ);
		assertThat(result, is("OK"));
		Long removed = embeddedJedis.lrem(SJIVE, 0, SROCK_THIS_TOWN);
		assertThat(removed, is(1L));
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removed = embeddedJedis.lrem(SJIVE, 0, SMY_JUANITA);
		assertThat(removed, is(0L));

	}

	@Test
	public void lpop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		String element = embeddedJedis.lpop(SJIVE);
		assertThat(element, is(SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.lpop(SJIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void rpop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		String element = embeddedJedis.rpop(SJIVE);
		assertThat(element, is(SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.rpop(SJIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void sadd_should_add_new_element_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		Long numberOfElements = embeddedJedis.setDatatypeOperations.scard(JIVE);
		assertThat(numberOfElements, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		embeddedJedis.sadd(SJIVE, SMY_JUANITA);
		numberOfElements = embeddedJedis.setDatatypeOperations.scard(JIVE);
		assertThat(numberOfElements, is(1L));

	}

	@Test
	public void smembers_should_return_empty_list_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		Set<byte[]> elements = embeddedJedis.smembers(JIVE);

		assertThat(elements, contains(ROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.smembers(JIVE);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void srem_should_remove_no_element_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.sadd(SJIVE, SMY_JUANITA);
		Long removedElements = embeddedJedis.srem(SJIVE, SMY_JUANITA);

		assertThat(removedElements, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.srem(SJIVE, SROCK_THIS_TOWN);
		assertThat(removedElements, is(0L));

	}

	@Test
	public void spop_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		String removedElements = embeddedJedis.spop(SJIVE);

		assertThat(removedElements, is(SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.spop(SJIVE);
		assertThat(removedElements, is(nullValue()));

	}

	@Test
	public void scard_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.sadd(SJIVE, SMY_JUANITA);
		Long numberOfElements = embeddedJedis.scard(SJIVE);

		assertThat(numberOfElements, is(2L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.scard(SJIVE);
		assertThat(numberOfElements, is(0L));

	}

	@Test
	public void sismember_should_return_false_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.sadd(SJIVE, SMY_JUANITA);
		boolean present = embeddedJedis.sismember(SJIVE, SROCK_THIS_TOWN);

		assertThat(present, is(true));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		present = embeddedJedis.sismember(SJIVE, SROCK_THIS_TOWN);

		assertThat(present, is(false));

	}

	@Test
	public void srandmember_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.sadd(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.sadd(SJIVE, SMY_JUANITA);
		String element = embeddedJedis.srandmember(SJIVE);

		assertThat(element, is(not(nullValue())));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		element = embeddedJedis.srandmember(SJIVE);
		assertThat(element, is(nullValue()));

	}

	@Test
	public void zadd_should_add_new_element_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		assertThat(embeddedJedis.sortsetDatatypeOperations.zcard(JIVE), is(1L));

	}

	@Test
	public void zadd_should_add_new_map_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		Map<Double, String> elements = new HashMap<Double, String>();
		elements.put(1D, SMY_JUANITA);

		embeddedJedis.zadd(SJIVE, elements);
		assertThat(embeddedJedis.sortsetDatatypeOperations.zcard(JIVE), is(1L));

	}

	@Test
	public void zrem_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrange(SJIVE, 0, -1);
		assertThat(elements, contains(SMY_JUANITA, SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrange(SJIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrange_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrange(SJIVE, 0, -1);
		assertThat(elements, contains(SMY_JUANITA, SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrange(SJIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zincrby_should_set_value_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SDURATION);

		double newValue = embeddedJedis.zincrby(SJIVE, 2D, SDURATION);
		assertThat(newValue, is(3D));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		newValue = embeddedJedis.zincrby(SJIVE, 2D, SDURATION);
		assertThat(newValue, is(2D));

	}

	@Test
	public void zrank_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);

		Long indexValue = embeddedJedis.zrank(SJIVE, SMY_JUANITA);
		assertThat(indexValue, is(0L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		indexValue = embeddedJedis.zrank(SJIVE, SMY_JUANITA);
		assertThat(indexValue, is(nullValue()));

	}

	@Test
	public void zrevrank_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);

		Long indexValue = embeddedJedis.zrank(SJIVE, SMY_JUANITA);
		assertThat(indexValue, is(0L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		indexValue = embeddedJedis.zrevrank(SJIVE, SMY_JUANITA);
		assertThat(indexValue, is(nullValue()));

	}

	@Test
	public void zrevrange_should_remove_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrevrange(SJIVE, 0, -1);
		assertThat(elements, contains(SROCK_THIS_TOWN, SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrange(SJIVE, 0, -1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrangeWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeWithScores(SJIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(SROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeWithScores(SJIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void zrevrangeWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeWithScores(SJIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeWithScores(SJIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}

	@Test
	public void zcard_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long numberOfElements = embeddedJedis.zcard(SJIVE);
		assertThat(numberOfElements, is(2L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.zcard(SJIVE);
		assertThat(numberOfElements, is(0L));

	}

	@Test
	public void zscore_should_return_null_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Double score = embeddedJedis.zscore(SJIVE, SROCK_THIS_TOWN);
		assertThat(score, is(2D));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		score = embeddedJedis.zscore(SJIVE, SROCK_THIS_TOWN);
		assertThat(score, is(nullValue()));

	}

	@Test
	public void sort_should_return_empty_list_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		List<String> elements = embeddedJedis.sort(SJIVE);
		assertThat(elements, contains(SMY_JUANITA, SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.sort(SJIVE);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zcount_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long numberOfElementsBetweenRange = embeddedJedis.zcount(SJIVE, 0, 1);
		assertThat(numberOfElementsBetweenRange, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElementsBetweenRange = embeddedJedis.zcount(SJIVE, 0, 1);
		assertThat(numberOfElementsBetweenRange, is(0L));

	}

	@Test
	public void zcount_with_infinite_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long numberOfElementsBetweenRange = embeddedJedis.zcount(SJIVE, "-inf", "+inf");
		assertThat(numberOfElementsBetweenRange, is(2L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElementsBetweenRange = embeddedJedis.zcount(SJIVE, "-inf", "+inf");
		assertThat(numberOfElementsBetweenRange, is(0L));

	}

	@Test
	public void zrangeByScore_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrangeByScore(SJIVE, 0, 1);
		assertThat(elements, contains(SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(SJIVE, 0, 1);
		assertThat(elements.size(), is(0));

	}

	@Test
	public void zrangeByScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrangeByScore(SJIVE, "-inf", "1");
		assertThat(elements, contains(SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(SJIVE, "-inf", "+inf");
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScore_with_infinite_and_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrangeByScore(SJIVE, "-inf", "1", 0, 1);
		assertThat(elements, contains(SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(SJIVE, "-inf", "+inf", 0, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrangeByScore(SJIVE, 0, 2, 1, 1);
		assertThat(elements, contains(SROCK_THIS_TOWN));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScore(SJIVE, 0, 2, 1, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, 0, 3);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, 0, 3);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_with_infinite_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, "-inf", "+inf");
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myJuanitaTuple, myRockThisTownTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, "-inf", "+inf");
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrangeByScoreWithScores_with_infinite_and_offset_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, "-inf", "+inf", 1, 1);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrangeByScoreWithScores(SJIVE, "-inf", "+inf", 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScore_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrevrangeByScore(SJIVE, 2, 0);
		assertThat(elements, contains(SROCK_THIS_TOWN, SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(SJIVE, 0, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrevrangeByScore(SJIVE, 2, 0, 1, 1);
		assertThat(elements, contains(SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(SJIVE, 2, 0, 1, 1);
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<String> elements = embeddedJedis.zrevrangeByScore(SJIVE, "+inf", "-inf");
		assertThat(elements, contains(SROCK_THIS_TOWN, SMY_JUANITA));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScore(SJIVE, "+inf", "-inf");
		assertThat(elements.size(), is(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScoreWithScores_should_return_no_elements_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, 3, 0);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, 3, 0);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, 3, 0, 1, 1);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		assertThat(elements, contains(myJuanitaTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, 3, 0, 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_infinite_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, "+inf", "-inf");
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		Tuple myRockThisTownTuple = new Tuple(ROCK_THIS_TOWN, 2D);
		assertThat(elements, contains(myRockThisTownTuple, myJuanitaTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, "+inf", "-inf");
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zrevrangeByScoreWithScore_with_infinite_and_offset_should_return_empty_set_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Set<Tuple> elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, "+inf", "-inf", 1, 1);
		Tuple myJuanitaTuple = new Tuple(SMY_JUANITA, 1D);
		assertThat(elements, contains(myJuanitaTuple));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		elements = embeddedJedis.zrevrangeByScoreWithScores(SJIVE, "+inf", "-inf", 1, 1);
		assertThat(elements, hasSize(0));

	}
	
	@Test
	public void zremrangeByRank_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByRank(SJIVE, 0, 0);
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByRank(SJIVE, 0, 1);
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void zremrangeByScore_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByScore(SJIVE, 0, 1);
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByScore(SJIVE, 0, 1);
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void zremrangeByScore__with_infinite_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.zadd(SJIVE, 1, SMY_JUANITA);
		embeddedJedis.zadd(SJIVE, 2, SROCK_THIS_TOWN);

		Long removedElements = embeddedJedis.zremrangeByScore(SJIVE, "-inf", "1");
		assertThat(removedElements, is(1L));

		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		removedElements = embeddedJedis.zremrangeByScore(SJIVE, "-inf", "1");
		assertThat(removedElements, is(0L));

	}
	
	@Test
	public void linsert_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		
		Long numberOfElements = embeddedJedis.linsert(SJIVE, LIST_POSITION.AFTER, SMY_JUANITA, SDARK_WALTZ);
		assertThat(numberOfElements, is(3L));
			
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.linsert(SJIVE, LIST_POSITION.AFTER, SMY_JUANITA, SDARK_WALTZ);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void lpushx_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		
		Long numberOfElements = embeddedJedis.lpushx(SJIVE, SMY_JUANITA);
		assertThat(numberOfElements, is(3L));
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.lpushx(SJIVE, SMY_JUANITA);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void rpushx_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.lpush(SJIVE, SROCK_THIS_TOWN);
		embeddedJedis.lpush(SJIVE, SMY_JUANITA);
		
		Long numberOfElements = embeddedJedis.rpushx(SJIVE, SMY_JUANITA);
		assertThat(numberOfElements, is(3L));
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		numberOfElements = embeddedJedis.rpushx(SJIVE, SMY_JUANITA);
		assertThat(numberOfElements, is(0L));

	}
	
	@Test
	public void setbit_should_return_zero_if_key_expired() throws InterruptedException {

		embeddedJedis.set(SJIVE, SROCK_THIS_TOWN);
		
		embeddedJedis.setbit(SJIVE, 2L, true);
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		boolean previousBit = embeddedJedis.setbit(SJIVE, 2L, true);
		assertThat(previousBit, is(false));

	}
	
	@Test
	public void getbit_should_return_zero_if_key_expired() throws InterruptedException {

		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		embeddedJedis.stringDatatypeOperations.simpleTypes.put(wrap(JIVE), wrap(values));
		assertThat(embeddedJedis.getbit(SJIVE, 8), is(true));
		
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		assertThat(embeddedJedis.getbit(SJIVE, 8), is(false));

	}
	
	@Test
	public void setrange_should_add_as_new_if_key_expired() throws InterruptedException {

		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		embeddedJedis.stringDatatypeOperations.simpleTypes.put(wrap(JIVE), wrap(values));
		Long newLength = embeddedJedis.setrange(JIVE, 0, values);
		
		assertThat(newLength, is(2L));
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		newLength = embeddedJedis.setrange(JIVE, 0, values);
		assertThat(newLength, is(2L));

	}
	
	@Test
	public void getrange_should_add_as_new_if_key_expired() throws InterruptedException {

		embeddedJedis.stringDatatypeOperations.append(JIVE, MY_JUANITA);
		
		String result = embeddedJedis.getrange(SJIVE, 1, 3);
		assertThat(result.length(), is(3));
		
		embeddedJedis.expire(SJIVE, 1);

		TimeUnit.MILLISECONDS.sleep(SLEEP_IN_MILLIS);

		result = embeddedJedis.getrange(SJIVE, 1, 3);
		assertThat(result.length(), is(0));

	}
	
}
