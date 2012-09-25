package com.lordofthejars.nosqlunit.redis.embedded;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.collection.IsMapContaining.hasValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.util.JedisByteHashMap;

public class WhenEmbeddedHashOperationsAreExecuted {

	private HashDatatypeOperations hashDatatypeOperations;
	
	private static final byte[] GROUP_NAME = "Queen".getBytes();
	private static final byte[] NEW_GROUP_NAME = "Queen+".getBytes();
	private static final byte[] ACTIVE_MEMBERS = "ReadyToPlay".getBytes();
	private static final byte[] NOT_IN_QUEEN_NOW = "SOLO".getBytes();
	private static final byte[] ALL_QUEEN = "AllQueen".getBytes();
	private static final byte[] VOCALIST = "Freddie Mercury".getBytes();
	private static final byte[] BASSIST = "John Deacon".getBytes();
	private static final byte[] GUITAR = "Brian May".getBytes();
	private static final byte[] DRUMER = "Roger Taylor".getBytes();
	private static final byte[] SUBSTITUTE_OF_FREDDIE = "Paul Rodgers".getBytes();
	private static final byte[] KEYBOARD = "Spike Edney".getBytes();	
	private static final byte[] MANAGER = "Jim Beach".getBytes();
	
	private static final byte[] FIELD_NAME = "name".getBytes();
	private static final byte[] FIELD_YEAR = "year".getBytes();
	private static final byte[] YEAR = "1946".getBytes();
	
	@Before
	public void setUp() {
		hashDatatypeOperations = new HashDatatypeOperations();
	}
	
	@Test
	public void hset_should_add_new_key_field_value() {
		Long result = hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		
		assertThat(result, is(1L));
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));
		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
	}
	
	@Test
	public void hset_should_add_new_field_value_in_already_defined_key() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		Long result = hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		assertThat(result, is(1L));
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));
		assertThat(row, hasKey(wrap(FIELD_YEAR)));
		assertThat(row, hasValue(wrap(YEAR)));
		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
	}
	
	@Test
	public void hset_should_update_field_value() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		Long result = hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, BASSIST);
		
		assertThat(result, is(0L));
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));
		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(BASSIST)));
		
	}
	
	@Test
	public void hget_should_return_required_data() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		byte[] vocalist = hashDatatypeOperations.hget(GROUP_NAME, FIELD_NAME);
		
		assertThat(vocalist, is(VOCALIST));
	}
	
	@Test
	public void hget_should_return_null_if_no_key_found() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		byte[] vocalist = hashDatatypeOperations.hget(NEW_GROUP_NAME, FIELD_NAME);
		
		assertThat(vocalist, is(nullValue()));
	}
	
	@Test
	public void hget_should_return_null_if_no_field_found() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		byte[] vocalist = hashDatatypeOperations.hget(GROUP_NAME, FIELD_YEAR);
		
		assertThat(vocalist, is(nullValue()));
	}
	
	@Test
	public void hdel_should_delete_field_value_in_already_defined_key() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Long result = hashDatatypeOperations.hdel(GROUP_NAME, FIELD_YEAR);
		assertThat(result, is(1L));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
		assertThat(row, not(hasKey(wrap(FIELD_YEAR))));
		assertThat(row, not(hasValue(wrap(YEAR))));
		
	}
	
	@Test
	public void hdel_should_delete_all_fields_value_in_already_defined_key() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Long result = hashDatatypeOperations.hdel(GROUP_NAME, FIELD_NAME, FIELD_YEAR);
		assertThat(result, is(2L));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, not(hasKey(wrap(FIELD_NAME))));
		assertThat(row, not(hasValue(wrap(VOCALIST))));
		
		assertThat(row, not(hasKey(wrap(FIELD_YEAR))));
		assertThat(row, not(hasValue(wrap(YEAR))));
		
	}
	
	@Test
	public void hdel_should_return_zero_if_key_not_defined() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
	
		Long result = hashDatatypeOperations.hdel(NEW_GROUP_NAME, FIELD_YEAR);
		assertThat(result, is(0L));
		
	}
	
	@Test
	public void hdel_should_return_zero_if_field_not_defined() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
	
		Long result = hashDatatypeOperations.hdel(GROUP_NAME, FIELD_YEAR);
		assertThat(result, is(0L));
		
	}
	
	@Test
	public void hexists_should_return_true_if_field_is_defined() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
	
		boolean result = hashDatatypeOperations.hexists(GROUP_NAME, FIELD_NAME);
		assertThat(result, is(true));
		
	}
	
	@Test
	public void hexists_should_return_false_if_field_is_not_defined() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
	
		boolean result = hashDatatypeOperations.hexists(GROUP_NAME, FIELD_YEAR);
		assertThat(result, is(false));
		
	}
	
	@Test
	public void hexists_should_return_false_if_key_is_not_defined() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
	
		boolean result = hashDatatypeOperations.hexists(NEW_GROUP_NAME, FIELD_YEAR);
		assertThat(result, is(false));
		
	}
	
	@Test
	public void hgetall_should_return_all_fields_and_values_of_key() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Map<byte[], byte[]> allFields = hashDatatypeOperations.hgetAll(GROUP_NAME);
		
		assertThat(allFields, hasKey(FIELD_NAME));
		assertThat(allFields, hasValue(VOCALIST));
		
		assertThat(allFields, hasKey(FIELD_YEAR));
		assertThat(allFields, hasValue(YEAR));
		
	}
	
	@Test
	public void hgetall_should_return_empty_map_if_key_not_exist() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Map<byte[], byte[]> allFields = hashDatatypeOperations.hgetAll(NEW_GROUP_NAME);
		
		assertThat(allFields.values(), hasSize(0));
		
	}
	
	@Test
	public void hincrby_should_increment_value() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Long newValue = hashDatatypeOperations.hincrBy(GROUP_NAME, FIELD_YEAR, 1L);
		
		assertThat(newValue, is((1947L)));
	}
	
	@Test
	public void hincrby_should_set_value_if_field_not_exist() {
		
		Long newValue = hashDatatypeOperations.hincrBy(GROUP_NAME, FIELD_YEAR, 5L);
		assertThat(newValue, is((5L)));
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void hincrby_should_throw_an_exception_if_field_is_not_number() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		
		hashDatatypeOperations.hincrBy(GROUP_NAME, FIELD_NAME, 5L);
	}
	
	@Test
	public void hkeys_should_return_all_columns() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Set<byte[]> columns = hashDatatypeOperations.hkeys(GROUP_NAME);
		assertThat(columns, hasSize(2));
		assertThat(columns, containsInAnyOrder(FIELD_NAME, FIELD_YEAR));
	}
	
	@Test
	public void hkeys_should_return_empty_list_if_no_key() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		Set<byte[]> columns = hashDatatypeOperations.hkeys(NEW_GROUP_NAME);
		assertThat(columns, hasSize(0));
	}
	
	@Test
	public void hlen_should_count_number_of_fields() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		long numberOfColumns = hashDatatypeOperations.hlen(GROUP_NAME);
		assertThat(numberOfColumns, is(2L));
	}
	
	@Test
	public void hlen_should_count_number_of_fields_if_key_not_exist() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		long numberOfColumns = hashDatatypeOperations.hlen(NEW_GROUP_NAME);
		assertThat(numberOfColumns, is(0L));
	}
	
	@Test
	public void hmget_should_return_all_values_of_given_fields() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		List<byte[]> columnValues = hashDatatypeOperations.hmget(GROUP_NAME, FIELD_NAME, FIELD_YEAR);
		
		assertThat(columnValues, hasSize(2));
		assertThat(columnValues, contains(VOCALIST, YEAR));
		
	}
	
	@Test
	public void hmget_should_return_null_values_for_key_not_found() {
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		List<byte[]> columnValues = hashDatatypeOperations.hmget(NEW_GROUP_NAME, FIELD_NAME, FIELD_YEAR);
		
		assertThat(columnValues, hasSize(2));
		assertThat(columnValues, contains(nullValue(), nullValue()));
		
	}
	
	@Test
	public void hmset_should_add_fields_to_key() {
		
		Map<byte[], byte[]> fieldValues = new JedisByteHashMap();
		
		fieldValues.put(FIELD_NAME, VOCALIST);
		fieldValues.put(FIELD_YEAR, YEAR);
		
		String result = hashDatatypeOperations.hmset(GROUP_NAME, fieldValues);
		assertThat(result, is("OK"));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
		assertThat(row, hasKey(wrap(FIELD_YEAR)));
		assertThat(row, hasValue(wrap(YEAR)));
		
	}
	
	@Test
	public void hmset_should_overwrite_fields_to_key() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		
		Map<byte[], byte[]> fieldValues = new JedisByteHashMap();
		
		fieldValues.put(FIELD_NAME, BASSIST);
		fieldValues.put(FIELD_YEAR, YEAR);
		
		String result = hashDatatypeOperations.hmset(GROUP_NAME, fieldValues);
		assertThat(result, is("OK"));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(BASSIST)));
		
		assertThat(row, hasKey(wrap(FIELD_YEAR)));
		assertThat(row, hasValue(wrap(YEAR)));
		
	}
	
	@Test
	public void hsetnx_should_add_field_if_not_previously_added() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		
		long result = hashDatatypeOperations.hsetnx(GROUP_NAME, FIELD_YEAR, YEAR);
		assertThat(result, is(1L));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
		assertThat(row, hasKey(wrap(FIELD_YEAR)));
		assertThat(row, hasValue(wrap(YEAR)));
		
	}
	
	@Test
	public void hsetnx_should_not_add_field_if_previously_added() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		
		long result = hashDatatypeOperations.hsetnx(GROUP_NAME, FIELD_NAME, BASSIST);
		assertThat(result, is(0L));
		
		Map<ByteBuffer, ByteBuffer> row = hashDatatypeOperations.hashElements.row(wrap(GROUP_NAME));

		assertThat(row, hasKey(wrap(FIELD_NAME)));
		assertThat(row, hasValue(wrap(VOCALIST)));
		
	}
	
	@Test
	public void hvals_should_return_all_values() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		List<byte[]> allValues = hashDatatypeOperations.hvals(GROUP_NAME);
		
		assertThat(allValues, hasSize(2));
		assertThat(allValues, containsInAnyOrder(VOCALIST, YEAR));
		
	}
	
	@Test
	public void hvals_should_return_empty_list_values_if_no_key() {
		
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_NAME, VOCALIST);
		hashDatatypeOperations.hset(GROUP_NAME, FIELD_YEAR, YEAR);
		
		List<byte[]> allValues = hashDatatypeOperations.hvals(NEW_GROUP_NAME);
		
		assertThat(allValues, hasSize(0));
		
	}
	
}
