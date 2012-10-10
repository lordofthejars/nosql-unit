package com.lordofthejars.nosqlunit.redis.embedded;


import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.lordofthejars.nosqlunit.redis.embedded.ExpirationDatatypeOperations.TtlState;

public class WhenKeysServerOperationsAreExecuted {

	private static final byte[] QUEEN_NAME = "Queen".getBytes();
	private static final byte[] QUEEN_PLUS_NAME = "Queen+".getBytes();
	private static final byte[] FISH_NAME = "Fish".getBytes();
	private static final byte[] MARILLION = "Marillion".getBytes();
	private static final byte[] MIRILLION = "Mirillion".getBytes();
	private static final byte[] PINK_FLOYD_NAME = "PinkFloyd".getBytes();
	private static final byte[] JAMES_NAME = "James".getBytes();
	private static final byte[] JOVANOTTI_NAME = "Jovanotti".getBytes();
	private static final byte[] ROGER_WATERS = "Roger Waters".getBytes();

	private static final byte[] A_KIND_OF_MAGIC = "A Kind Of Magic".getBytes();
	private static final byte[] KAYLEIGH = "Kayleigh".getBytes();
	private static final byte[] WISH_YOU_WERE_HERE = "Wish You Were Here".getBytes();
	private static final byte[] LAID = "Laid".getBytes();
	private static final byte[] A_TE = "A Te".getBytes();
	private static final byte[] SUGAR_MICE = "Sugar Mice".getBytes();
	private static final byte[] MOTHER = "Mother".getBytes();

	private static final byte[] ONE = "1.0".getBytes();
	private static final byte[] TWO = "2.0".getBytes();
	

	private static final byte[] SONG = "SONG".getBytes();

	private HashDatatypeOperations hashDatatypeOperations;
	private ListDatatypeOperations listDatatypeOperations;
	private SetDatatypeOperations setDatatypeOperations;
	private SortsetDatatypeOperations sortsetDatatypeOperations;
	private StringDatatypeOperations stringDatatypeOperations;

	private KeysServerOperations keysServerOperations;

	@Before
	public void setUp() {

		hashDatatypeOperations = new HashDatatypeOperations();
		hashDatatypeOperations.hset(QUEEN_NAME, SONG, A_KIND_OF_MAGIC);

		listDatatypeOperations = new ListDatatypeOperations();
		listDatatypeOperations.lpush(FISH_NAME, KAYLEIGH);

		setDatatypeOperations = new SetDatatypeOperations();
		setDatatypeOperations.sadd(PINK_FLOYD_NAME, WISH_YOU_WERE_HERE);

		sortsetDatatypeOperations = new SortsetDatatypeOperations();
		sortsetDatatypeOperations.zadd(JAMES_NAME, 1, LAID);

		stringDatatypeOperations = new StringDatatypeOperations();
		stringDatatypeOperations.append(JOVANOTTI_NAME, A_TE);

		keysServerOperations = KeysServerOperations.createKeysServerOperations(hashDatatypeOperations,
				listDatatypeOperations, setDatatypeOperations, sortsetDatatypeOperations, stringDatatypeOperations);

	}
	
	@Test
	public void dbsize_should_return_number_of_all_keys() {
		
		Long numberOfKeys = keysServerOperations.dbSize();
		assertThat(numberOfKeys, is(5L));
		
	}

	@Test
	public void flush_all_should_delete_all_elements() {
		
		String result = keysServerOperations.flushAll();
		assertThat(result, is("OK"));
		
		Long numberOfKeys = keysServerOperations.dbSize();
		assertThat(numberOfKeys, is(0L));
		
	}
	
	@Test
	public void flush_db_should_delete_all_elements() {
		
		String result = keysServerOperations.flushDB();
		assertThat(result, is("OK"));
		
		Long numberOfKeys = keysServerOperations.dbSize();
		assertThat(numberOfKeys, is(0L));
		
	}
	
	@Test
	public void del_should_delete_given_keys() {
		
		Long numberOfRemoved = keysServerOperations.del(PINK_FLOYD_NAME, QUEEN_NAME);
		assertThat(numberOfRemoved, is(2L));
		
		assertThat(hashDatatypeOperations.hexists(QUEEN_NAME, SONG), is(false));
		assertThat(setDatatypeOperations.exists(PINK_FLOYD_NAME), is(false));
		
		Long numberOfKeys = keysServerOperations.dbSize();
		assertThat(numberOfKeys, is(3L));
		
	}
	
	@Test
	public void exists_should_return_true_if_key_exists() {
		
		Boolean existsKey = keysServerOperations.exists(JAMES_NAME);
		assertThat(existsKey, is(true));
	}
	
	@Test
	public void exists_should_return_false_if_key_exists() {
		
		Boolean existsKey = keysServerOperations.exists(SONG);
		assertThat(existsKey, is(false));
	}
	
	@Test
	public void rename_should_rename_an_inserted_key() {
		
		String result = keysServerOperations.rename(QUEEN_NAME, QUEEN_PLUS_NAME);
		assertThat(result, is("OK"));
		
		assertThat(hashDatatypeOperations.hexists(QUEEN_PLUS_NAME, SONG), is(true));
		assertThat(hashDatatypeOperations.hexists(QUEEN_NAME, SONG), is(false));
		
	}
	
	@Test
	public void rename_should_rename_an_inserted_key_to_an_already_exist_key_overwritting_previous_values() {
		
		listDatatypeOperations.lpush(MARILLION, SUGAR_MICE);
		
		String result = keysServerOperations.rename(FISH_NAME, MARILLION);
		assertThat(result, is("OK"));
		
		assertThat(listDatatypeOperations.llen(MARILLION), is(1L));
		assertThat(listDatatypeOperations.lpop(MARILLION), is(KAYLEIGH));
		assertThat(listDatatypeOperations.llen(FISH_NAME), is(0L));
		
	}
	
	@Test
	public void rename_should_rename_an_inserted_key_to_an_already_exist_key_overwritting_previous_values_of_different_types() {
		
		String result = keysServerOperations.rename(FISH_NAME, ROGER_WATERS);
		assertThat(result, is("OK"));
		
		assertThat(listDatatypeOperations.llen(ROGER_WATERS), is(1L));
		assertThat(listDatatypeOperations.lpop(ROGER_WATERS), is(KAYLEIGH));
		assertThat(listDatatypeOperations.llen(FISH_NAME), is(0L));
		
		assertThat(setDatatypeOperations.scard(ROGER_WATERS), is(0L));
		
	}
	
	@Test
	public void rename_should_return_ko_if_old_key_and_new_key_is_the_same() {
		
		String result = keysServerOperations.rename(QUEEN_PLUS_NAME, QUEEN_PLUS_NAME);
		assertThat(result, is("-"));
		
	}
	
	@Test
	public void rename_should_return_ko_if_old_key_does_not_exist() {
		
		String result = keysServerOperations.rename(QUEEN_PLUS_NAME, FISH_NAME);
		assertThat(result, is("-"));
		
	}
	
	@Test
	public void renamex_should_rename_an_inserted_key_to_new_key_if_not_exists() {
		
		Long result = keysServerOperations.renamenx(QUEEN_NAME, QUEEN_PLUS_NAME);
		assertThat(result, is(1L));
		
		assertThat(hashDatatypeOperations.hexists(QUEEN_PLUS_NAME, SONG), is(true));
		assertThat(hashDatatypeOperations.hexists(QUEEN_NAME, SONG), is(false));
		
	}
	
	@Test
	public void renamex_should_not_rename_an_inserted_key_to_new_key_if_new_key_exists() {
		
		setDatatypeOperations.sadd(ROGER_WATERS, MOTHER);
		
		Long result = keysServerOperations.renamenx(ROGER_WATERS, PINK_FLOYD_NAME);
		assertThat(result, is(0L));
		
		assertThat(setDatatypeOperations.scard(ROGER_WATERS), is(1L));
		assertThat(setDatatypeOperations.scard(PINK_FLOYD_NAME), is(1L));
		
	}
	
	@Test 
	public void expire_should_add_expiration_time_to_key() {
		Long result = keysServerOperations.expire(FISH_NAME, 10);
		assertThat(result, is(1L));
		assertThat(listDatatypeOperations.expirationsInMillis.get(wrap(FISH_NAME)), is(greaterThan(System.currentTimeMillis())));
		assertThat(listDatatypeOperations.timedoutState(FISH_NAME), is(TtlState.NOT_EXPIRED));
		
	}
	
	@Test
	public void expire_should_not_add_expiration_to_not_inserted_keys() {
		Long result = keysServerOperations.expire(QUEEN_PLUS_NAME, 10);
		assertThat(result, is(0L));
	}
	
	
	@Test
	public void expire_at_should_add_expiration_date_to_key() {
		
		Long result = keysServerOperations.expire(PINK_FLOYD_NAME, (int)TimeUnit.SECONDS.convert(System.currentTimeMillis()+10000, TimeUnit.MILLISECONDS));
		assertThat(result, is(1L));
		assertThat(setDatatypeOperations.expirationsInMillis.get(wrap(PINK_FLOYD_NAME)), is(greaterThan(System.currentTimeMillis())));
		assertThat(setDatatypeOperations.timedoutState(PINK_FLOYD_NAME), is(TtlState.NOT_EXPIRED));
		
	}
	
	@Test
	public void expire_at_should_not_add_expiration_date_to_not_inserted_key() {
		
		Long result = keysServerOperations.expire(ROGER_WATERS, (int)TimeUnit.SECONDS.convert(System.currentTimeMillis()+10000, TimeUnit.MILLISECONDS));
		assertThat(result, is(0L));
		
	}
	
	@Test
	public void keys_should_return_keys_matching_pattern() {
		
		listDatatypeOperations.lpush(MARILLION, SUGAR_MICE);
		listDatatypeOperations.lpush(MIRILLION, SUGAR_MICE);
		
		Set<byte[]> keys = keysServerOperations.keys("M[ai]rillion".getBytes());
		assertThat(keys, containsInAnyOrder(MARILLION, MIRILLION));
		assertThat(listDatatypeOperations.keys().size(), is(3));
		
	}
	
	@Test
	public void persist_should_remove_ttl_to_key() {
		
		keysServerOperations.expire(JOVANOTTI_NAME, 10);
		Long result = keysServerOperations.persist(JOVANOTTI_NAME);
		assertThat(result, is(1L));
		
		assertThat(stringDatatypeOperations.timedoutState(JOVANOTTI_NAME), is(TtlState.NOT_MANAGED));
		
	}
	
	@Test
	public void ttl_should_return_time_to_expire() {
		
		keysServerOperations.expire(JAMES_NAME, 10);
		Long timeToExpire = keysServerOperations.ttl(JAMES_NAME);
		
		assertThat(timeToExpire, is(10L));
		
	}
	
	@Test
	public void ttl_should_return_no_time_to_expire_if_key_cannot_expire() {
		
		keysServerOperations.expire(JAMES_NAME, 10);
		Long timeToExpire = keysServerOperations.ttl(QUEEN_PLUS_NAME);
		
		assertThat(timeToExpire, is(ExpirationDatatypeOperations.NO_EXPIRATION));
		
	}
	
	@Test
	public void rename_should_rename_ttl_time_to_expire() {
		
		listDatatypeOperations.lpush(MARILLION, SUGAR_MICE);
		
		keysServerOperations.expire(FISH_NAME, 10);
		keysServerOperations.rename(FISH_NAME, MARILLION);
		
		assertThat(listDatatypeOperations.expirationsInMillis.get(wrap(MARILLION)), is(greaterThan(System.currentTimeMillis())));
		
	}
	
	@Test
	public void type_should_return_hash_for_hash_type() {
		String type = keysServerOperations.type(QUEEN_NAME);
		assertThat(type, is(HashDatatypeOperations.HASH));
	}
	
	@Test
	public void type_should_return_list_for_list_type() {
		String type = keysServerOperations.type(FISH_NAME);
		assertThat(type, is(ListDatatypeOperations.LIST));
	}
	
	@Test
	public void type_should_return_set_for_set_type() {
		String type = keysServerOperations.type(PINK_FLOYD_NAME);
		assertThat(type, is(SetDatatypeOperations.SET));
	}
	
	@Test
	public void type_should_return_zset_for_sortset_type() {
		String type = keysServerOperations.type(JAMES_NAME);
		assertThat(type, is(SortsetDatatypeOperations.ZSET));
	}
	
	@Test
	public void type_should_return_string_for_string_type() {
		String type = keysServerOperations.type(JOVANOTTI_NAME);
		assertThat(type, is(StringDatatypeOperations.STRING));
	}
	
	@Test
	public void sort_should_sort_list_with_numberable() {
	
		listDatatypeOperations.lpush(SONG, ONE, TWO);
		List<byte[]> orderedNumbers = keysServerOperations.sort(SONG);
		assertThat(orderedNumbers, contains(ONE, TWO));
		
	}
	
	@Test
	public void sort_should_return_list_as_is_for_non_numerable() {
		listDatatypeOperations.lpush(FISH_NAME, SUGAR_MICE);
		List<byte[]> orderedNumbers = keysServerOperations.sort(FISH_NAME);
		assertThat(orderedNumbers, contains(SUGAR_MICE, KAYLEIGH));
	}
	
	@Test
	public void sort_should_sort_set_with_numberable() {
		setDatatypeOperations.sadd(SONG, TWO, ONE);
		List<byte[]> orderedNumbers = keysServerOperations.sort(SONG);
		assertThat(orderedNumbers, contains(ONE, TWO));
	}
	@Test
	public void sort_should_return_set_as_is_for_non_numerable() {
		setDatatypeOperations.sadd(PINK_FLOYD_NAME, MOTHER);
		List<byte[]> orderedNumbers = keysServerOperations.sort(PINK_FLOYD_NAME);
		assertThat(orderedNumbers, contains(WISH_YOU_WERE_HERE, MOTHER));
	}
	
	@Test
	public void sort_should_sort_sortset_with_numberable() {
		sortsetDatatypeOperations.zadd(SONG, 1, TWO);
		sortsetDatatypeOperations.zadd(SONG, 2, ONE);
		
		List<byte[]> orderedNumbers = keysServerOperations.sort(SONG);
		assertThat(orderedNumbers, contains(ONE, TWO));
		
	}
	@Test
	public void sort_should_return_sortset_as_is_for_non_numerable() {
		sortsetDatatypeOperations.zadd(JAMES_NAME, 0, SUGAR_MICE);
		List<byte[]> orderedNumbers = keysServerOperations.sort(JAMES_NAME);
		assertThat(orderedNumbers, contains(SUGAR_MICE, LAID));
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void sort_should_throw_an_exception_in_hashes() {
		 keysServerOperations.sort(QUEEN_NAME);
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void sort_should_throw_an_exception_in_strings() {
		 keysServerOperations.sort(JOVANOTTI_NAME);
	}
	
}
