package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;

import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class WhenEmbeddedStringOperationsAreExecuted {

	private static final byte[] AAA = "aaa".getBytes();
	private static final byte[] FIVE = "five".getBytes();
	private static final byte[] FOUR = "four".getBytes();
	private static final byte[] FISH_MARILLION = "MaFishion".getBytes();
	private static final byte[] MARILLION_FISH = "MarilliFish".getBytes();
	private static final byte[] MARILLION = "Marillion".getBytes();
	private static final byte[] MARILLION2 = "Marillion".getBytes();
	private static final byte[] ERIC_CLAPTON = "Eric_Clapton".getBytes();
	private static final byte[] SUB_ERIC = "ric_".getBytes();
	private static final byte[] THREE = "three".getBytes();
	private static final byte[] FISH = "Fish".getBytes();
	private static final byte[] TWO = "two".getBytes();
	private static final byte[] QUEEN = "Queen".getBytes();
	private static final byte[] ONE = "one".getBytes();
	private static final byte[] ONE_NUMBER = "1".getBytes();
	private static final byte[] TWO_NUMBER = "2".getBytes();
	private static final byte[] THREE_NUMBER = "3".getBytes();
	private static final byte[] ZERO_NUMBER = "0".getBytes();
	private static final byte[] NEGATIVE_ONE_NUMBER = "-1".getBytes();
	private static final byte[] NEGATIVE_TWO_NUMBER = "-2".getBytes();
	private static final byte[] NEGATIVE_THREE_NUMBER = "-3".getBytes();
	
	private StringDatatypeOperations stringDatatypeOperations;
	
	@Before
	public void setUp() {
		stringDatatypeOperations = new StringDatatypeOperations();
	}
	
	@Test
	public void mset_should_add_all_elements(){
		String result = stringDatatypeOperations.mset(ONE, QUEEN, TWO, FISH, THREE, ERIC_CLAPTON);
		
		assertThat("OK", is(result));
	}
	
	@Test
	public void mset_should_exception(){
		String result = stringDatatypeOperations.mset(ONE, QUEEN, TWO, FISH, THREE);
		assertThat(null, is(result));
	}
	
	@Test
	public void mset_should_overwrite(){
		String result = stringDatatypeOperations.mset(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		
		assertThat("OK", is(result));
	}
	
	@Test
	public void msetnx_should_add_all_elements(){
		Long result = stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		assertThat(1L, is(result));
	}
	
	@Test
	public void msetnx_should_overwrite_element(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		Long result = stringDatatypeOperations.msetnx(FOUR, QUEEN, TWO, FISH);
		assertThat(0L, is(result));
	}

	@Test
	public void strlen_key_does_not_exist() {
		int result = stringDatatypeOperations.strlen(ONE);
		
		assertThat(0, is(result));
	}
	
	@Test
	public void strlen_length() {
		stringDatatypeOperations.set(ONE, QUEEN);
		int result = stringDatatypeOperations.strlen(ONE);
		
		assertThat(5, is(result));
	}
	
	@Test
	public void rename_should_rename_key(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		String result = stringDatatypeOperations.rename(ONE, FOUR);
		assertThat("OK", is(result));
	}
	
	@Test
	public void rename_should_newkey_equals_oldkey(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		String result = stringDatatypeOperations.rename(ONE, ONE);
		assertThat(null, is(result));
	}
	
	@Test
	public void rename_should_oldkey_does_not_exist(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, MARILLION);
		String result = stringDatatypeOperations.rename(FOUR, FIVE);
		assertThat(null, is(result));
	}
	
	@Test
	public void mget_should_return_all_values(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, "".getBytes());
		List<byte[]> result = stringDatatypeOperations.mget(ONE, TWO, THREE, FOUR);
		assertThat(result, containsInAnyOrder(QUEEN, FISH, "".getBytes(), null));
	}
	
	
	@Test
	public void get_should_return_value(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, "".getBytes());
		byte[] result = stringDatatypeOperations.get(ONE);
		assertThat(QUEEN, is(result));
	}
	
	@Test
	public void get_should_key_does_not_exist(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, "".getBytes());
		byte[] result = stringDatatypeOperations.get(FOUR);
		assertThat(result, nullValue());
	}
	
	@Test
	public void getSet_should_return_value(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, AAA);
		byte[] result = stringDatatypeOperations.getSet(THREE, MARILLION);
		assertThat(AAA, is(result));
	}
	
	@Test
	public void getSet_should_key_does_not_exist(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH, THREE, "".getBytes());
		byte[] result = stringDatatypeOperations.getSet(FOUR, MARILLION);
		assertThat(result, nullValue());
	}
	
	@Test
	public void set_should_return_ok(){
		String result = stringDatatypeOperations.set(ONE, QUEEN);
		assertThat("OK", is(result));
	}
	
	@Test
	public void setnx_should_set_value(){
		stringDatatypeOperations.msetnx(ONE, QUEEN);
		Long result = stringDatatypeOperations.setnx(TWO, FISH);
		assertThat(1L, is(result));
	}
	
	@Test
	public void setnx_should_exist_key(){
		stringDatatypeOperations.msetnx(ONE, QUEEN, TWO, FISH);
		Long result = stringDatatypeOperations.setnx(ONE, MARILLION);
		assertThat(0L, is(result));
	}
	
	@Test
	public void append_should_append_already_inserted_data() {
		
		stringDatatypeOperations.mset(ONE, QUEEN);
		Long totalLength = stringDatatypeOperations.append(ONE, FISH);
		assertThat(totalLength, is(9L));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(ONE));
		assertThat(byteBuffer.array(), is("QueenFish".getBytes()));
		
	}
	
	@Test
	public void append_should_insert_data_if_key_does_not_exist() {
		
		Long totalLength = stringDatatypeOperations.append(ONE, FISH);
		assertThat(totalLength, is(4L));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(ONE));
		assertThat(byteBuffer.array(), is("Fish".getBytes()));
		
	}
	
	@Test
	public void incr_should_increment_value_by_one() {
		
		stringDatatypeOperations.append(ONE, ONE_NUMBER);
		Long increment = stringDatatypeOperations.incr(ONE);
		assertThat(increment, is(2L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(ONE)).array(), is(TWO_NUMBER));
		
	}
	
	@Test
	public void incr_should_increment_value_by_one_in_keys_not_found() {
		
		stringDatatypeOperations.append(ONE, ONE_NUMBER);
		Long increment = stringDatatypeOperations.incr(TWO);
		assertThat(increment, is(1L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(TWO)).array(), is(ONE_NUMBER));
		
	}
	
	@Test
	public void incrby_should_increment_value_by_value() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.incrBy(QUEEN, 2);
		assertThat(increment, is(3L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(QUEEN)).array(), is(THREE_NUMBER));
		
	}
	
	@Test
	public void incrby_should_increment_value_by_value_in_keys_not_found() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.incrBy(FISH, 3L);
		assertThat(increment, is(3L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(FISH)).array(), is(THREE_NUMBER));
		
	}
	
	@Test
	public void decr_should_decrement_value_by_one() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.decr(QUEEN);
		assertThat(increment, is(0L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(QUEEN)).array(), is(ZERO_NUMBER));
		
	}
	
	@Test
	public void decr_should_decrement_value_by_one_in_keys_not_found() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.decr(FISH);
		assertThat(increment, is(-1L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(FISH)).array(), is(NEGATIVE_ONE_NUMBER));
		
	}
	
	@Test
	public void decrby_should_decrement_value_by_one() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.decrBy(QUEEN, 2L);
		assertThat(increment, is(-1L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(QUEEN)).array(), is(NEGATIVE_ONE_NUMBER));
		
	}
	
	@Test
	public void decrby_should_decrement_value_by_value_in_keys_not_found() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.decrBy(FISH, 3L);
		assertThat(increment, is(-3L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(FISH)).array(), is(NEGATIVE_THREE_NUMBER));
		
	}
	
	@Test
	public void getbit_should_return_number_of_bit_in_string() {
		
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		boolean bit = stringDatatypeOperations.getbit(QUEEN, 8);
		assertThat(bit, is(true));
		
	}
	
	@Test
	public void getbit_should_return_number_of_bit_in_string_if_0() {
		
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		boolean bit = stringDatatypeOperations.getbit(QUEEN, 9);
		assertThat(bit, is(false));
		
	}
	
	@Test
	public void getbit_should_return_false_if_offset_out_of_bounds() {
		
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		boolean bit = stringDatatypeOperations.getbit(QUEEN, 25);
		assertThat(bit, is(false));
		
	}
	
	@Test
	public void getbit_should_return_false_if_key_not_exists() {
		
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		boolean bit = stringDatatypeOperations.getbit(ONE, 1);
		assertThat(bit, is(false));
		
	}
	
	@Test
	public void decrby_should_increment_value_if_negative_decrement() {
		
		stringDatatypeOperations.append(QUEEN, ONE_NUMBER);
		Long increment = stringDatatypeOperations.decrBy(QUEEN, -1L);
		assertThat(increment, is(2L));
		
		assertThat(stringDatatypeOperations.simpleTypes.get(wrap(QUEEN)).array(), is(TWO_NUMBER));
		
	}
	
	@Test
	public void getrange_should_return_bytes_between_range() {
		
		stringDatatypeOperations.append(QUEEN, ERIC_CLAPTON);
		
		byte[] suberic = stringDatatypeOperations.getrange(QUEEN, 1, 4);
		assertThat(suberic, is(SUB_ERIC));
		
	}
	
	@Test
	public void getrange_should_return_bytes_between_range_with_negative() {
		
		stringDatatypeOperations.append(QUEEN, ERIC_CLAPTON);
		
		byte[] suberic = stringDatatypeOperations.getrange(QUEEN, 1, -8);
		assertThat(suberic, is(SUB_ERIC));
		
	}
	
	@Test
	public void getrange_should_return_bytes_until_end_if_upper_overflow() {
		
		stringDatatypeOperations.append(QUEEN, ERIC_CLAPTON);
		
		byte[] suberic = stringDatatypeOperations.getrange(QUEEN, 0, 40);
		assertThat(suberic, is(ERIC_CLAPTON));
		
	}
	
	@Test
	public void setbit_should_set_bit_to_offset() {
	
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		Boolean previousBit = stringDatatypeOperations.setbit(QUEEN, 0, ONE_NUMBER);
		assertThat(previousBit, is(false));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(QUEEN));
		byte[] currentValues = byteBuffer.array();
		
		assertThat(BitsUtils.getBit(currentValues, 0), is(1));
		
	}
	
	@Test
	public void setbit_should_set_bit_to_offset_and_extend_if_not_fill() {
	
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		Boolean previousBit = stringDatatypeOperations.setbit(QUEEN, 20, ONE_NUMBER);
		assertThat(previousBit, is(false));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(QUEEN));
		byte[] currentValues = byteBuffer.array();
		
		assertThat(currentValues.length, is(3));
		assertThat(BitsUtils.getBit(currentValues, 20), is(1));
		
	}

	@Test
	public void setbit_should_set_bit_to_offset_if_key_does_not_exist() {
	
		byte[] values = new byte[2];
		BitsUtils.setBit(values, 8, 1);
		
		stringDatatypeOperations.simpleTypes.put(wrap(QUEEN), wrap(values));
		
		Boolean previousBit = stringDatatypeOperations.setbit(FISH, 5, ONE_NUMBER);
		assertThat(previousBit, is(false));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(FISH));
		byte[] currentValues = byteBuffer.array();
		
		assertThat(currentValues.length, is(1));
		assertThat(BitsUtils.getBit(currentValues, 5), is(1));
		
	}
	
	@Test
	public void setrange_should_add_elements_into_range() {
		
		stringDatatypeOperations.append(QUEEN, MARILLION);
		Long newLength = stringDatatypeOperations.setrange(QUEEN, 2, FISH);
		
		assertThat(newLength, is(9L));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(QUEEN));
		byte[] currentValues = byteBuffer.array();
		
		assertThat(currentValues, is(FISH_MARILLION));
		
	}
	
	@Test
	public void setrange_should_add_elements_into_range_with_extend() {
		
		stringDatatypeOperations.append(QUEEN, MARILLION2);
		Long newLength = stringDatatypeOperations.setrange(QUEEN, 7, FISH);
		
		assertThat(newLength, is(11L));
		
		ByteBuffer byteBuffer = stringDatatypeOperations.simpleTypes.get(wrap(QUEEN));
		byte[] currentValues = byteBuffer.array();
		
		assertThat(currentValues, is(MARILLION_FISH));
		
	}
	
}