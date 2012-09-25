package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.util.Set;


import org.junit.Before;
import org.junit.Test;

public class WhenEmbeddedSetOperationsAreExecuted {

	private SetDatatypeOperations setDatatypeOperations;
	
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
	
	@Before
	public void setUp() {
		setDatatypeOperations = new SetDatatypeOperations();
	}
	
	@Test
	public void sadd_should_add_new_value_to_set() {
		
		long response = setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		assertThat(response, is(4L));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
	
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		
	}
	
	@Test
	public void sadd_should_not_add_already_value_to_set() {
		
		long response = setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		assertThat(response, is(4L));
		
		response = setDatatypeOperations.sadd(GROUP_NAME, VOCALIST);
		assertThat(response, is(0L));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
	
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		
	}

	@Test
	public void sadd_should_add_a_new_element_in_already_created_set() {
		
		long response = setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		assertThat(response, is(4L));
		
		response = setDatatypeOperations.sadd(GROUP_NAME, MANAGER);
		assertThat(response, is(1L));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(MANAGER)), is(true));
	
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(5));
		
	}
	
	@Test
	public void scard_should_return_number_of_inserted_elements_for_this_key() {
		
		long response = setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		assertThat(response, is(4L));
		
		long numberOfInsertedElements = setDatatypeOperations.scard(GROUP_NAME);
		assertThat(numberOfInsertedElements, is(4L));
		
	}
	
	@Test
	public void scard_should_return_zero_if_no_key_inserted() {
		long numberOfInsertedElements = setDatatypeOperations.scard(GROUP_NAME);
		assertThat(numberOfInsertedElements, is(0L));
	}
	
	@Test
	public void sdiff_should_calculate_difference_between_first_and_the_others() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		Set<byte[]> originalMembersNotInNewGroup = setDatatypeOperations.sdiff(GROUP_NAME, NEW_GROUP_NAME);
		assertThat(originalMembersNotInNewGroup, hasSize(2));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(originalMembersNotInNewGroup, containsInAnyOrder(VOCALIST, BASSIST));
		
	}
	
	@Test
	public void sdiff_store_should_calculate_difference_between_first_and_the_others_and_store() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		long numberOriginalMembersNotInNewGroup = setDatatypeOperations.sdiffstore(NOT_IN_QUEEN_NOW, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOriginalMembersNotInNewGroup, is(2L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(NOT_IN_QUEEN_NOW)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NOT_IN_QUEEN_NOW), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NOT_IN_QUEEN_NOW), wrap(BASSIST)), is(true));
		
	}
	
	@Test
	public void sdiff_store_should_calculate_difference_between_first_and_the_others_and_store_with_overwrite() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NOT_IN_QUEEN_NOW, SUBSTITUTE_OF_FREDDIE);
		
		long numberOriginalMembersNotInNewGroup = setDatatypeOperations.sdiffstore(NOT_IN_QUEEN_NOW, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOriginalMembersNotInNewGroup, is(2L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(NOT_IN_QUEEN_NOW)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NOT_IN_QUEEN_NOW), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NOT_IN_QUEEN_NOW), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NOT_IN_QUEEN_NOW), wrap(SUBSTITUTE_OF_FREDDIE)), is(false));
		
	}
	
	@Test
	public void sinter_should_calculate_intersaction_between_first_and_the_others_and_store() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		Set<byte[]> originalMembersActive = setDatatypeOperations.sinter(GROUP_NAME, NEW_GROUP_NAME);
		assertThat(originalMembersActive, hasSize(2));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(NEW_GROUP_NAME)), hasSize(3));
		assertThat(originalMembersActive, containsInAnyOrder(GUITAR, DRUMER));
		
	}
	
	@Test
	public void sinter_store_should_calculate_intersaction_between_first_and_the_others_and_store() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		long numberOriginalMembersActive = setDatatypeOperations.sinterstore(ACTIVE_MEMBERS, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOriginalMembersActive, is(2L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(ACTIVE_MEMBERS)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ACTIVE_MEMBERS), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ACTIVE_MEMBERS), wrap(DRUMER)), is(true));
		
	}
	
	@Test
	public void sinter_store_should_calculate_intersaction_between_first_and_the_others_and_store_with_overwrite() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		setDatatypeOperations.sadd(ACTIVE_MEMBERS, KEYBOARD);
		
		long numberOriginalMembersActive = setDatatypeOperations.sinterstore(ACTIVE_MEMBERS, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(numberOriginalMembersActive, is(2L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(ACTIVE_MEMBERS)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ACTIVE_MEMBERS), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ACTIVE_MEMBERS), wrap(DRUMER)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ACTIVE_MEMBERS), wrap(KEYBOARD)), is(false));
	}
	
	@Test
	public void sismember_should_return_true_if_set_contains_element() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		Boolean isFreddieAMemeberOfQueen = setDatatypeOperations.sismember(GROUP_NAME, VOCALIST);
		
		assertThat(isFreddieAMemeberOfQueen, is(true));
	}
	
	@Test
	public void sismember_should_return_false_if_set_not_contains_element() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		Boolean isPaulRodgersAMemeberOfQueen = setDatatypeOperations.sismember(GROUP_NAME, SUBSTITUTE_OF_FREDDIE);
		
		assertThat(isPaulRodgersAMemeberOfQueen, is(false));
	}
	
	@Test
	public void smembers_should_return_all_members_of_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Set<byte[]> groupMembers = setDatatypeOperations.smembers(GROUP_NAME);
		
		assertThat(groupMembers, hasSize(4));
		assertThat(groupMembers, containsInAnyOrder(VOCALIST, BASSIST, GUITAR, DRUMER));
		
	}
	
	@Test
	public void smembers_should_return_empty_set_if_no_members_for_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Set<byte[]> groupMembers = setDatatypeOperations.smembers(NEW_GROUP_NAME);
		
		assertThat(groupMembers, hasSize(0));
		
	}
	
	@Test
	public void smove_should_move_element_to_not_existing_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Long result = setDatatypeOperations.smove(GROUP_NAME, NEW_GROUP_NAME, GUITAR);
		assertThat(result, is(1L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(3));
		assertThat(setDatatypeOperations.setElements.get(wrap(NEW_GROUP_NAME)), hasSize(1));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NEW_GROUP_NAME), wrap(GUITAR)), is(true));
		
	}
	
	@Test
	public void smove_should_move_element_to_existing_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR);
		
		setDatatypeOperations.sadd(NEW_GROUP_NAME, DRUMER);
		
		Long result = setDatatypeOperations.smove(GROUP_NAME, NEW_GROUP_NAME, GUITAR);
		assertThat(result, is(1L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.get(wrap(NEW_GROUP_NAME)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NEW_GROUP_NAME), wrap(DRUMER)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NEW_GROUP_NAME), wrap(GUITAR)), is(true));
		
	}
	
	@Test
	public void smove_should_remove_only_element_if_it_is_already_in_destination() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		setDatatypeOperations.sadd(NEW_GROUP_NAME, DRUMER);
		
		Long result = setDatatypeOperations.smove(GROUP_NAME, NEW_GROUP_NAME, DRUMER);
		assertThat(result, is(1L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(3));
		assertThat(setDatatypeOperations.setElements.get(wrap(NEW_GROUP_NAME)), hasSize(1));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(NEW_GROUP_NAME), wrap(DRUMER)), is(true));
		
	}
	
	@Test
	public void smove_should_not_be_executed_if_no_source_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR);
		
		
		Long result = setDatatypeOperations.smove(NEW_GROUP_NAME, GROUP_NAME, DRUMER);
		assertThat(result, is(0L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(3));
	}
	
	@Test
	public void smove_should_not_be_executed_if_no_element_in_source_key() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Long result = setDatatypeOperations.smove(GROUP_NAME, NEW_GROUP_NAME, KEYBOARD);
		assertThat(result, is(0L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));

	}
	
	@Test
	public void spop_should_remove_one_random_element() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		byte[] removedBandMember = setDatatypeOperations.spop(GROUP_NAME);
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(3));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(removedBandMember)), is(false));
		
	}
	
	@Test
	public void spop_should_return_null_if_key_does_not_exist() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		byte[] removedBandMember = setDatatypeOperations.spop(NEW_GROUP_NAME);
		
		assertThat(removedBandMember, is(nullValue()));
		
	}
	
	@Test
	public void srandmember_should_return_a_random_element() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		byte[] removedBandMember = setDatatypeOperations.srandmember(GROUP_NAME);
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(removedBandMember, is(notNullValue()));
		
	}
	
	@Test
	public void srandmember_should_return_null_if_key_does_not_exist() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		byte[] removedBandMember = setDatatypeOperations.srandmember(NEW_GROUP_NAME);
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(removedBandMember, is(nullValue()));
		
	}
	
	@Test
	public void srem_should_remove_all_specified_elements_from_set() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Long numberOfRemovedMembers = setDatatypeOperations.srem(GROUP_NAME, VOCALIST, BASSIST);
		
		assertThat(numberOfRemovedMembers, is(2L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(2));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
		
	}
	
	@Test
	public void srem_should_return_zero_if_no_element_in_set() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Long numberOfRemovedMembers = setDatatypeOperations.srem(GROUP_NAME, KEYBOARD);
		
		assertThat(numberOfRemovedMembers, is(0L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
		
	}
	
	@Test
	public void srem_should_return_zero_if_no_key_found() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		
		Long numberOfRemovedMembers = setDatatypeOperations.srem(NEW_GROUP_NAME, KEYBOARD);
		
		assertThat(numberOfRemovedMembers, is(0L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(GROUP_NAME), wrap(DRUMER)), is(true));
		
	}
	
	
	@Test
	public void sunion_should_calculate_union_between_first_and_the_others_and_store() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		Set<byte[]> allMembersOfQueen = setDatatypeOperations.sunion(GROUP_NAME, NEW_GROUP_NAME);
		assertThat(allMembersOfQueen, hasSize(5));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(NEW_GROUP_NAME)), hasSize(3));
		assertThat(allMembersOfQueen, containsInAnyOrder(GUITAR, DRUMER, VOCALIST, BASSIST, SUBSTITUTE_OF_FREDDIE));
		
	}
	
	@Test
	public void sunion_store_should_calculate_union_between_first_and_the_others_and_store() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		
		long allMembersOfQueen = setDatatypeOperations.sunionstore(ALL_QUEEN, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(allMembersOfQueen, is(5L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(ALL_QUEEN)), hasSize(5));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(DRUMER)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(SUBSTITUTE_OF_FREDDIE)), is(true));
		
	}
	
	@Test
	public void sunion_store_should_calculate_union_between_first_and_the_others_and_store_with_overwrite() {
		
		setDatatypeOperations.sadd(GROUP_NAME, VOCALIST, BASSIST, GUITAR, DRUMER);
		setDatatypeOperations.sadd(NEW_GROUP_NAME, SUBSTITUTE_OF_FREDDIE, GUITAR, DRUMER);
		setDatatypeOperations.sadd(ALL_QUEEN, KEYBOARD);
		
		long allMembersOfQueen = setDatatypeOperations.sunionstore(ALL_QUEEN, GROUP_NAME, NEW_GROUP_NAME);
		assertThat(allMembersOfQueen, is(5L));
		
		assertThat(setDatatypeOperations.setElements.get(wrap(GROUP_NAME)), hasSize(4));
		assertThat(setDatatypeOperations.setElements.get(wrap(ALL_QUEEN)), hasSize(5));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(GUITAR)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(DRUMER)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(VOCALIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(BASSIST)), is(true));
		assertThat(setDatatypeOperations.setElements.containsEntry(wrap(ALL_QUEEN), wrap(SUBSTITUTE_OF_FREDDIE)), is(true));
	}
}
