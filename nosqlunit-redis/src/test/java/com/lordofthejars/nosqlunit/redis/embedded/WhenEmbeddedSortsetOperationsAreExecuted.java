package com.lordofthejars.nosqlunit.redis.embedded;


import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.ZParams;
import redis.clients.jedis.ZParams.Aggregate;

import com.lordofthejars.nosqlunit.redis.embedded.SortsetDatatypeOperations.ScoredByteBuffer;

public class WhenEmbeddedSortsetOperationsAreExecuted {

	private SortsetDatatypeOperations sortsetDatatypeOperations;
	
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
		sortsetDatatypeOperations = new SortsetDatatypeOperations();
	}
	
	@Test
	public void zadd_should_add_multiple_fields_ordered_by_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		long result = sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		assertThat(result, is(4L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 3D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 4D);
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer));
		
	}
	
	@Test
	public void zadd_should_update_score_if_member_already_exists() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, VOCALIST);
		
		long result = sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		assertThat(result, is(4L));
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();

		newGroupMembers.put(1D, VOCALIST);
		newGroupMembers.put(6D, KEYBOARD);
		
		result = sortsetDatatypeOperations.zadd(GROUP_NAME, newGroupMembers);
		assertThat(result, is(1L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 3D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 4D);
		ScoredByteBuffer keyboard = ScoredByteBuffer.createScoredByteBuffer(wrap(KEYBOARD), 6D);
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer, keyboard));
		
	}
	

	@Test
	public void zcard_should_count_number_of_elements() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
	
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		long numberOfMembers = sortsetDatatypeOperations.zcard(GROUP_NAME);
		assertThat(numberOfMembers, is(4L));
	}
	
	@Test
	public void zcard_should_count_0_if_no_key() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
	
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		long numberOfMembers = sortsetDatatypeOperations.zcard(NEW_GROUP_NAME);
		assertThat(numberOfMembers, is(0L));
	}
	
	@Test
	public void zcount_should_count_number_of_elements_between_inclusive_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long numberOfMembersBetweenScore = sortsetDatatypeOperations.zcount(GROUP_NAME, 2, 4);
		
		assertThat(numberOfMembersBetweenScore, is(3L));
		
	}
	
	@Test
	public void zcount_should_count_number_of_elements_between_exclusive_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long numberOfMembersBetweenScore = sortsetDatatypeOperations.zcount(GROUP_NAME, "(1".getBytes(), "(4".getBytes());
		
		assertThat(numberOfMembersBetweenScore, is(2L));
		
	}
	
	@Test
	public void zcount_should_count_number_of_elements_with_infinite_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long numberOfMembersBetweenScore = sortsetDatatypeOperations.zcount(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes());
		
		assertThat(numberOfMembersBetweenScore, is(4L));
		
	}
	
	@Test
	public void zcount_should_count_zero_if_no_elements_found() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();

		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long numberOfMembersBetweenScore = sortsetDatatypeOperations.zcount(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes());
		
		assertThat(numberOfMembersBetweenScore, is(0L));
		
	}
	
	@Test
	public void zincrby_should_increment_score_of_element() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(3D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double newScore = sortsetDatatypeOperations.zincrby(GROUP_NAME, 2, GUITAR);
		
		assertThat(newScore,is(5D));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 4D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, contains(vocalist, bassist, drumer, guitar));
		
	}
	
	@Test
	public void zincrby_should_add_element_if_member_is_not_present() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double newScore = sortsetDatatypeOperations.zincrby(GROUP_NAME, 3D, GUITAR);
		assertThat(newScore, is(3D));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 3D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 4D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer));
		
	}
	
	@Test
	public void zincrby_should_add_element_if_key_is_not_present() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double newScore = sortsetDatatypeOperations.zincrby(GROUP_NAME, 3D, GUITAR);
		assertThat(newScore, is(3D));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 3D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 4D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(4));
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer));
		
	}
	
	@Test
	public void zunionstore_should_store_union_of_elements_with_sum_score_by_default() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(4D, DRUMER);
		groupMembers.put(3D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(4D, DRUMER);
		newGroupMembers.put(3D, GUITAR);
		newGroupMembers.put(5D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, new ZParams(), GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 6D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 8D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 5D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, bassist, substitute, guitar, drumer));
	}
	
	@Test
	public void zunionstore_should_overwrite_union_of_elements_with_sum_score_by_default() {
		
		Map<Double, byte[]> allMembers = new HashMap<Double, byte[]>();
		allMembers.put(4D, KEYBOARD);
		
		sortsetDatatypeOperations.zadd(ALL_QUEEN, allMembers);
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(4D, DRUMER);
		groupMembers.put(3D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(4D, DRUMER);
		newGroupMembers.put(3D, GUITAR);
		newGroupMembers.put(5D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, new ZParams(), GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 6D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 8D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 5D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, bassist, substitute, guitar, drumer));
	}
	
	@Test
	public void zunionstore_should_store_union_of_elements_with_sum_of_weighted_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(4D, DRUMER);
		groupMembers.put(3D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(2D, GUITAR);
		newGroupMembers.put(1D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.weights(2, 3);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 2D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 4D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 12D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 17D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 3D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, substitute, bassist, guitar, drumer));
	}
	
	@Test
	public void zunionstore_should_store_union_of_elements_with_min_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(4D, GUITAR);
		newGroupMembers.put(8D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.aggregate(Aggregate.MIN);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 4D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 3D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 8D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, bassist, drumer, guitar, substitute));
	}
	
	@Test
	public void zunionstore_should_store_union_of_elements_with_max_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(4D, GUITAR);
		newGroupMembers.put(8D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.aggregate(Aggregate.MAX);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 7D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 8D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer, substitute));
	}
	
	@Test
	public void zunionstore_should_store_union_of_elements_with_aggregation_and_weights() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(4D, GUITAR);
		newGroupMembers.put(8D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.aggregate(Aggregate.MAX);
		zParams.weights(2, 3);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zunionstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(5L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 2D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 4D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 12D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 14D);
		ScoredByteBuffer substitute = ScoredByteBuffer.createScoredByteBuffer(wrap(SUBSTITUTE_OF_FREDDIE), 24D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(5));
		assertThat(orderedMembers, contains(vocalist, bassist, guitar, drumer, substitute));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void zunionstore_should_store_union_of_elements_with_invalid_weights() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(4D, GUITAR);
		newGroupMembers.put(8D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.aggregate(Aggregate.MAX);
		zParams.weights(2, 3, 4);
		
		sortsetDatatypeOperations.zunionstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
	}
	
	@Test
	public void zinterstore_should_store_union_of_elements_with_aggregation_and_weights() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Map<Double, byte[]> newGroupMembers = new HashMap<Double, byte[]>();
		newGroupMembers.put(3D, DRUMER);
		newGroupMembers.put(4D, GUITAR);
		newGroupMembers.put(8D, SUBSTITUTE_OF_FREDDIE);
		
		sortsetDatatypeOperations.zadd(NEW_GROUP_NAME, newGroupMembers);
		
		ZParams zParams = new ZParams();
		zParams.aggregate(Aggregate.SUM);
		zParams.weights(2, 3);
		
		Long numberOfAllMembers = sortsetDatatypeOperations.zinterstore(ALL_QUEEN, zParams, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOfAllMembers, is(2L));
		
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 22D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 23D);
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(ALL_QUEEN));
		assertThat(orderedMembers, hasSize(2));
		assertThat(orderedMembers, contains(guitar, drumer));
	}
	
	@Test
	public void zrange_should_return_elements_between_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> lastElement = sortsetDatatypeOperations.zrange(GROUP_NAME, 2, 3);
		
		assertThat(lastElement, hasSize(1));
		assertThat(lastElement, contains(GUITAR));
		
	}
	
	@Test
	public void zrange_should_return_elements_between_range_inclusive() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> lastElement = sortsetDatatypeOperations.zrange(GROUP_NAME, 1, 3);
		
		assertThat(lastElement, hasSize(3));
		assertThat(lastElement, contains(BASSIST, DRUMER, GUITAR));
		
	}
	
	@Test
	public void zrange_should_return_all_elements_between_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrange(GROUP_NAME, 0, -1);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(VOCALIST, BASSIST, GUITAR));
		
	}
	
	@Test
	public void zrange_should_return_elements_between_negative_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> elements = sortsetDatatypeOperations.zrange(GROUP_NAME, -2, -1);
		
		assertThat(elements, hasSize(2));
		assertThat(elements, contains(BASSIST, GUITAR));
		
	}
	
	@Test
	public void zrange_should_return_elements_between_negative_and_positives_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> elements = sortsetDatatypeOperations.zrange(GROUP_NAME, 1, -2);
		
		assertThat(elements, hasSize(1));
		assertThat(elements, contains(BASSIST));
		
	}
	
	@Test
	public void zrange_should_return_all_scored_elements_between_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeWithScores(GROUP_NAME, 0, -1);
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(vocalist, bassist, guitar));
		
	}
	
	@Test
	public void zrange_by_score_should_return_all_elements_between_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 0, 6);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(VOCALIST, BASSIST, GUITAR));
		
	}
	
	@Test
	public void zrange_by_score_should_return_no_elements_scores_outside_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 7, 10);
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score_should_return_all_elements_with_infinite_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes());
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(VOCALIST, BASSIST, GUITAR));
		
	}
	
	@Test
	public void zrange_by_score_should_return_all_elements_with_exclusive_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "(1".getBytes(), "(5".getBytes());
		
		assertThat(allElements, hasSize(1));
		assertThat(allElements, contains(BASSIST));
		
	}
	
	@Test
	public void zrange_by_score_should_return_elements_with_exclusive_min_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "(0".getBytes(), "(5".getBytes());
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(VOCALIST, BASSIST));
		
	}
	
	@Test
	public void zrange_by_score_should_return_no_elements_with_exclusive_max_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "(-2".getBytes(), "(-1".getBytes());
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score_should_return_no_elements_with_inverted_min_max_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 2, 0);
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score_should_return_elements_limited_by_offset() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 0, 6, 1, 2);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(BASSIST, GUITAR));
	}
	
	@Test
	public void zrange_by_score_should_return_elements_limited_by_offset_and_outrange_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 0, 6, 2, 10);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(GUITAR, DRUMER));
	}
	
	@Test
	public void zrange_by_score_should_return_elements_limited_by_offset_and_negative_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 0, 6, 2, -10);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(GUITAR, DRUMER));
	}
	
	@Test
	public void zrange_by_score_should_return_no_elements_limited_by_offset_greater_than_number_elements() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, 0, 6, 10, -10);
		
		assertThat(allElements, hasSize(0));
	}
	
	
	@Test
	public void zrange_by_score_should_return_elements_limited_by_offset_with_infintie_parameter() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes(), 1, 3);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(BASSIST, GUITAR, DRUMER));
	}
	
	@Test
	public void zrange_by_score_should_return_elements_limited_with_exclusive_parameters() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "(0".getBytes(), "(2".getBytes(), 0, 1);
		
		assertThat(allElements, hasSize(1));
		assertThat(allElements, contains(VOCALIST));
	}
	
	@Test
	public void zrange_by_score_should_return_elements_limited_by_offset_and_outrange_count_with_infinite_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrangeByScore(GROUP_NAME, "1".getBytes(), "+inf".getBytes(), 0, 10);
		
		assertThat(allElements, hasSize(4));
		assertThat(allElements, contains(VOCALIST, BASSIST, GUITAR, DRUMER));
	}
	
	
	@Test
	public void zrange_by_score_with_score_should_return_all_elements_between_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 0, 6);
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(vocalist, bassist, guitar));
		
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_no_elements_scores_outside_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 7, 10);
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_all_elements_with_infinite_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes());
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(vocalist, bassist, guitar));
		
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_all_elements_with_exclusive_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "(1".getBytes(), "(5".getBytes());
		
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		
		assertThat(allElements, hasSize(1));
		assertThat(allElements, contains(bassist));
		
	}
	
	@Test
	public void zrange_by_score__with_score_should_return_elements_with_exclusive_min_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "(0".getBytes(), "(5".getBytes());
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(vocalist, bassist));
		
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_no_elements_with_exclusive_max_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "(-2".getBytes(), "(-1".getBytes());
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score__with_score_should_return_no_elements_with_inverted_min_max_outrange_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 2, 0);
		
		assertThat(allElements, hasSize(0));
		
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_by_offset() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 0, 6, 1, 2);
		
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(bassist, guitar));
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_by_offset_and_outrange_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 0, 6, 2, 10);
		
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(guitar, drumer));
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_by_offset_and_negative_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 0, 6, 2, -10);
		
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(2));
		assertThat(allElements, contains(guitar, drumer));
	}
	
	@Test
	public void zrange_by_score__with_score_should_return_no_elements_limited_by_offset_greater_than_number_elements() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, 0, 6, 10, -10);
		
		assertThat(allElements, hasSize(0));
	}
	
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_by_offset_with_infintie_parameter() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "-inf".getBytes(), "+inf".getBytes(), 1, 3);
		
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(bassist, guitar, drumer));
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_with_exclusive_parameters() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "(0".getBytes(), "(2".getBytes(), 0, 1);
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		
		assertThat(allElements, hasSize(1));
		assertThat(allElements, contains(vocalist));
	}
	
	@Test
	public void zrange_by_score_with_score_should_return_elements_limited_by_offset_and_outrange_count_with_infinite_count() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> allElements = sortsetDatatypeOperations.zrangeByScoreWithScores(GROUP_NAME, "1".getBytes(), "+inf".getBytes(), 0, 10);
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		
		assertThat(allElements, hasSize(4));
		assertThat(allElements, contains(vocalist, bassist, guitar, drumer));
	}
	
	@Test
	public void zrank_should_return_index_of_given_element() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrank(GROUP_NAME, GUITAR);
		assertThat(index, is(2L));
		
	}
	
	@Test
	public void zrank_should_return_null_if_key_not_found() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrank(NEW_GROUP_NAME, GUITAR);
		assertThat(index, is(nullValue()));
		
	}
	
	@Test
	public void zrank_should_return_null_if_element_not_found() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrank(GROUP_NAME, KEYBOARD);
		assertThat(index, is(nullValue()));
		
	}
	
	@Test
	public void zrem_should_remove_all_passed_elements() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zrem(GROUP_NAME, GUITAR, VOCALIST);
		assertThat(removedElements, is(2L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(2));
		
	}
	
	@Test
	public void zrem_should_not_remove_elements_of_none_existing_key() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zrem(NEW_GROUP_NAME, GUITAR, VOCALIST);
		assertThat(removedElements, is(0L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(4));
		
	}
	
	@Test
	public void zrem_should_not_remove_elements_not_present() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zrem(GROUP_NAME, GUITAR, KEYBOARD);
		assertThat(removedElements, is(1L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(3));
		
	}
	
	@Test
	public void zremrangeByRank_should_remove_elements_by_rank() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zremrangeByRank(GROUP_NAME, 1, 2);
		assertThat(removedElements, is(2L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(2));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);

		assertThat(orderedMembers, contains(vocalist, drumer));
		
	}
	
	@Test
	public void zremrangeByRank_should_remove_all_elements_using_negative_ranks() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zremrangeByRank(GROUP_NAME, 0, -1);
		assertThat(removedElements, is(4L));
		
		Collection<ScoredByteBuffer> orderedMembers = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(orderedMembers, hasSize(0));
		
	}
	
	@Test
	public void zremrangeByScore_should_remove_elements_between_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zremrangeByScore(GROUP_NAME, 2, 4);
		assertThat(removedElements, is(1L));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		
		Collection<ScoredByteBuffer> members = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(members, hasSize(3));
		assertThat(members, contains(vocalist, guitar, drumer));
		
	}
	
	@Test
	public void zremrangeByScore_should_remove_elements_between_infinite_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long removedElements = sortsetDatatypeOperations.zremrangeByScore(GROUP_NAME, "-inf".getBytes(), "2".getBytes());
		assertThat(removedElements, is(2L));
		
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		
		Collection<ScoredByteBuffer> members = sortsetDatatypeOperations.sortset.get(wrap(GROUP_NAME));
		assertThat(members, hasSize(2));
		assertThat(members, contains(guitar, drumer));
		
	}
	
	@Test
	public void zrevrange_should_return_reverse_elements_between_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> lastElement = sortsetDatatypeOperations.zrange(GROUP_NAME, 2, 3);
		
		assertThat(lastElement, hasSize(1));
		assertThat(lastElement, contains(GUITAR));
		
	}
	
	@Test
	public void zrevrange_should_return_inverted_elements_between_range_inclusive() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(4D, DRUMER);
		groupMembers.put(5D, GUITAR);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> lastElement = sortsetDatatypeOperations.zrevrange(GROUP_NAME, 1, 3);
		
		assertThat(lastElement, hasSize(3));
		assertThat(lastElement, contains(DRUMER, BASSIST, VOCALIST));
		
	}
	
	@Test
	public void zrevrange_should_return_all_inverted_elements_between_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> allElements = sortsetDatatypeOperations.zrevrange(GROUP_NAME, 0, -1);
		
		assertThat(allElements, hasSize(3));
		assertThat(allElements, contains(GUITAR, BASSIST, VOCALIST));
		
	}
	
	@Test
	public void zrevrange_should_return_inverted_elements_between_negative_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> elements = sortsetDatatypeOperations.zrevrange(GROUP_NAME, -2, -1);
		
		assertThat(elements, hasSize(2));
		assertThat(elements, contains(BASSIST, VOCALIST));
		
	}
	
	@Test
	public void zrevrange_should_return_inverted_elements_between_negative_and_positives_range() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> elements = sortsetDatatypeOperations.zrevrange(GROUP_NAME, 1, -2);
		
		assertThat(elements, hasSize(1));
		assertThat(elements, contains(BASSIST));
		
	}
	
	@Test
	public void zrevrange_should_return_inverted_elements_between_negative_and_positives_range_with_scores() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> elements = sortsetDatatypeOperations.zrevrangeWithScores(GROUP_NAME, 1, -2);
		
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		
		assertThat(elements, hasSize(1));
		assertThat(elements, contains(bassist));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_scope_number() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);
		
		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, 7, 2);
		assertThat(zrevrangeByScore, hasSize(4));
		
		assertThat(zrevrangeByScore, contains(KEYBOARD, DRUMER, GUITAR, BASSIST));
		
	}
	
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_infinite_scope() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, KEYBOARD);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, "+inf".getBytes(), "-inf".getBytes());
		assertThat(zrevrangeByScore, hasSize(5));
		
		assertThat(zrevrangeByScore, contains(KEYBOARD, DRUMER, GUITAR, BASSIST, VOCALIST));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_with_exclusive_scope() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, KEYBOARD);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, "2".getBytes(), "(1	".getBytes());
		assertThat(zrevrangeByScore, hasSize(1));
		
		assertThat(zrevrangeByScore, contains(BASSIST));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_empty_elements_with_consecutive_exclusive_scope() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(7D, KEYBOARD);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, "(2".getBytes(), "(1	".getBytes());
		assertThat(zrevrangeByScore, hasSize(0));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_range_scope_with_limit() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, 5, 0, 1, 2);
		assertThat(zrevrangeByScore, hasSize(2));
		
		assertThat(zrevrangeByScore, contains(BASSIST, VOCALIST));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_infinite_scope_with_limit() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<byte[]> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScore(GROUP_NAME, "+inf".getBytes(), "-inf".getBytes(), 1, 2);
		assertThat(zrevrangeByScore, hasSize(2));
		
		assertThat(zrevrangeByScore, contains(DRUMER, GUITAR));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_range_scope_with_limit_and_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScoreWithScores(GROUP_NAME, 5, 0, 1, 2);
		assertThat(zrevrangeByScore, hasSize(2));
		
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		
		assertThat(zrevrangeByScore, contains(bassist, vocalist));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_infinite_scope_with_limit_and_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScoreWithScores(GROUP_NAME, "+inf".getBytes(), "-inf".getBytes(), 1, 2);
		assertThat(zrevrangeByScore, hasSize(2));
		
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(zrevrangeByScore, contains(drumer, guitar));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_range_scope_and_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScoreWithScores(GROUP_NAME, 5, 0);
		assertThat(zrevrangeByScore, hasSize(3));
		
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		
		assertThat(zrevrangeByScore, contains(guitar, bassist, vocalist));
		
	}
	
	@Test
	public void zrevrangeByScore_should_return_inverted_elements_between_infinite_scope_and_score() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(1D, VOCALIST);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(6D, DRUMER);
		groupMembers.put(7D, KEYBOARD);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Set<ScoredByteBuffer> zrevrangeByScore = sortsetDatatypeOperations.zrevrangeByScoreWithScores(GROUP_NAME, "+inf".getBytes(), "-inf".getBytes());
		assertThat(zrevrangeByScore, hasSize(5));
		
		ScoredByteBuffer vocalist = ScoredByteBuffer.createScoredByteBuffer(wrap(VOCALIST), 1D);
		ScoredByteBuffer bassist = ScoredByteBuffer.createScoredByteBuffer(wrap(BASSIST), 2D);
		ScoredByteBuffer drumer = ScoredByteBuffer.createScoredByteBuffer(wrap(DRUMER), 6D);
		ScoredByteBuffer guitar = ScoredByteBuffer.createScoredByteBuffer(wrap(GUITAR), 5D);
		ScoredByteBuffer keyboard = ScoredByteBuffer.createScoredByteBuffer(wrap(KEYBOARD), 7D);
		
		assertThat(zrevrangeByScore, contains(keyboard, drumer, guitar, bassist, vocalist));
		
	}
	
	@Test
	public void zrevrank_should_return_index_of_given_element() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrevrank(GROUP_NAME, GUITAR);
		assertThat(index, is(1L));
		
	}
	
	@Test
	public void zrevrank_should_return_null_if_key_not_found() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrank(NEW_GROUP_NAME, GUITAR);
		assertThat(index, is(nullValue()));
		
	}
	
	@Test
	public void zrevrank_should_return_null_if_element_not_found() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Long index = sortsetDatatypeOperations.zrank(GROUP_NAME, KEYBOARD);
		assertThat(index, is(nullValue()));
		
	}
	
	@Test
	public void zscore_should_return_the_score_of_member() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double score = sortsetDatatypeOperations.zscore(GROUP_NAME, GUITAR);
		assertThat(score, is(5D));
		
	}
	
	@Test
	public void zscore_should_return_null_if_member_does_not_exist() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double score = sortsetDatatypeOperations.zscore(GROUP_NAME, KEYBOARD);
		assertThat(score, is(nullValue()));
		
	}
	
	@Test
	public void zscore_should_return_null_if_key_does_not_exist() {
		
		Map<Double, byte[]> groupMembers = new HashMap<Double, byte[]>();
		groupMembers.put(6D, DRUMER);
		groupMembers.put(5D, GUITAR);
		groupMembers.put(2D, BASSIST);
		groupMembers.put(1D, VOCALIST);

		sortsetDatatypeOperations.zadd(GROUP_NAME, groupMembers);
		
		Double score = sortsetDatatypeOperations.zscore(NEW_GROUP_NAME, GUITAR);
		assertThat(score, is(nullValue()));
		
	}
	
}
