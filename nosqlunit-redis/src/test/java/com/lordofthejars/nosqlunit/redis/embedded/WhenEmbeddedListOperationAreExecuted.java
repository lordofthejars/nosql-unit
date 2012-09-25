package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.lordofthejars.nosqlunit.redis.embedded.ListDatatypeOperations.ListPositionEnum;

public class WhenEmbeddedListOperationAreExecuted {

	private ListDatatypeOperations listDatatypeOperations;

	private static final byte[] GROUP_NAME = "Queen".getBytes();
	private static final byte[] NEW_GROUP_NAME = "Queen+".getBytes();
	private static final byte[] VOCALIST = "Freddie Mercury".getBytes();
	private static final byte[] BASSIST = "John Deacon".getBytes();
	private static final byte[] GUITAR = "Brian May".getBytes();
	private static final byte[] DRUMER = "Roger Taylor".getBytes();
	private static final byte[] KEYBOARD = "Spike Edney".getBytes();

	@Before
	public void setUp() {
		listDatatypeOperations = new ListDatatypeOperations();
	}

	@Test
	public void blpop_should_wait_until_one_element_is_available() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				finishTest.countDown();

			}
		});

		consumer.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(2);
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void brpop_should_wait_until_one_element_is_available_and_return_last_one() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.brpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				finishTest.countDown();

			}
		});

		consumer.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(2);
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void blpop_should_return_null_if_timeout_expires() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.blpop(2, GROUP_NAME);
				assertThat(waitObject, is(nullValue()));
				finishTest.countDown();

			}
		});

		consumer.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(5);
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void brpop_should_return_null_if_timeout_expires() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.brpop(2, GROUP_NAME);
				assertThat(waitObject, is(nullValue()));
				finishTest.countDown();

			}
		});

		consumer.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(5);
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void blpop_should_return_one_element_if_it_is_already_available() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				countDownLatch.countDown();
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					fail();
				}
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				finishTest.countDown();

			}
		});

		consumer.start();
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void brpop_should_return_last_element_if_it_is_already_available() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
				listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

				countDownLatch.countDown();
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					fail();
				}
				List<byte[]> waitObject = listDatatypeOperations.brpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(BASSIST));
				finishTest.countDown();

			}
		});

		consumer.start();
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void blpop_should_return_one_element_if_it_is_already_available_from_multiple_keys()
			throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				countDownLatch.countDown();
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					fail();
				}
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, NEW_GROUP_NAME, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				finishTest.countDown();

			}
		});

		consumer.start();
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void blpop_should_wait_until_one_element_from_multiple_keys_is_available() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		producer.start();

		Thread consumer = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, NEW_GROUP_NAME, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				finishTest.countDown();

			}
		});

		consumer.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(2);
		countDownLatch.countDown();

		finishTest.await();

	}

	@Test
	public void blpop_should_notify_clients_waiting_for_same_key_available() throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final CountDownLatch finishTest = new CountDownLatch(2);
		final CountDownLatch nextProduct = new CountDownLatch(1);

		Thread producer = new Thread(new Runnable() {

			public void run() {
				try {
					countDownLatch.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
					nextProduct.await();
					listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
				} catch (InterruptedException e) {
					fail();
				}
			}
		});

		Thread consumer1 = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				nextProduct.countDown();
				finishTest.countDown();

			}
		});

		Thread consumer2 = new Thread(new Runnable() {

			public void run() {
				List<byte[]> waitObject = listDatatypeOperations.blpop(0, GROUP_NAME);
				assertThat(waitObject, hasSize(2));
				assertThat(waitObject.get(0), equalTo(GROUP_NAME));
				assertThat(waitObject.get(1), equalTo(VOCALIST));
				nextProduct.countDown();
				finishTest.countDown();

			}
		});

		consumer1.start();
		consumer2.start();

		// Safe time to assure that getAndWait is called before producer.
		TimeUnit.SECONDS.sleep(2);
		countDownLatch.countDown();
		producer.start();

		finishTest.await();

	}

	@Test
	public void lindex_should_return_element_insert_in_index() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] guitar = listDatatypeOperations.lindex(GROUP_NAME, 1);
		assertThat(guitar, equalTo(GUITAR));
	}

	@Test
	public void lindex_should_return_null_if_key_not_insert() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] guitar = listDatatypeOperations.lindex(NEW_GROUP_NAME, 1);
		assertThat(guitar, nullValue());
	}

	@Test
	public void lindex_should_return_null_if_index_out_of_bounds() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] guitar = listDatatypeOperations.lindex(GROUP_NAME, 3);
		assertThat(guitar, nullValue());
	}

	@Test
	public void lindex_should_return_element_with_negative_index() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] bassist = listDatatypeOperations.lindex(GROUP_NAME, -1);
		assertThat(bassist, equalTo(BASSIST));
	}

	@Test
	public void linsert_should_add_element_before_pivot_element() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.linsert(GROUP_NAME, ListPositionEnum.BEFORE, GUITAR, DRUMER);
		assertThat(sizeOfGroup, is(4L));

		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(DRUMER), wrap(GUITAR), wrap(BASSIST)));
	}

	@Test
	public void linsert_should_add_element_after_pivot_element() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.linsert(GROUP_NAME, ListPositionEnum.AFTER, GUITAR, DRUMER);
		assertThat(sizeOfGroup, is(4L));

		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(GUITAR), wrap(DRUMER), wrap(BASSIST)));
	}

	@Test
	public void linsert_should_do_nothing_if_key_not_found() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.linsert(NEW_GROUP_NAME, ListPositionEnum.AFTER, GUITAR, DRUMER);
		assertThat(sizeOfGroup, is(0L));

	}

	@Test
	public void linsert_should_return__1_if_pivot_not_found() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.linsert(GROUP_NAME, ListPositionEnum.AFTER, KEYBOARD, DRUMER);
		assertThat(sizeOfGroup, is(-1L));

	}

	@Test
	public void llen_should_return_length_of_inserted_elements() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.llen(GROUP_NAME);
		assertThat(sizeOfGroup, is(3L));
	}

	@Test
	public void llen_should_return_0_if_no_key_found() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		long sizeOfGroup = listDatatypeOperations.llen(NEW_GROUP_NAME);
		assertThat(sizeOfGroup, is(0L));
	}

	@Test
	public void lpop_should_return_left_element() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] firstMember = listDatatypeOperations.lpop(GROUP_NAME);
		assertThat(firstMember, is(VOCALIST));

		assertThat(listDatatypeOperations.blockingMultimap.size(wrap(GROUP_NAME)), is(2));
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(GUITAR), wrap(BASSIST)));
	}

	@Test
	public void lpop_should_return_null_if_no_element_element() {

		byte[] firstMember = listDatatypeOperations.lpop(GROUP_NAME);
		assertThat(firstMember, is(nullValue()));
	}

	@Test
	public void rpop_should_return_right_element() {

		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));

		byte[] firstMember = listDatatypeOperations.rpop(GROUP_NAME);
		assertThat(firstMember, is(BASSIST));

		assertThat(listDatatypeOperations.blockingMultimap.size(wrap(GROUP_NAME)), is(2));
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(GUITAR)));
	}

	@Test
	public void rpop_should_return_null_if_no_element_element() {

		byte[] firstMember = listDatatypeOperations.rpop(GROUP_NAME);
		assertThat(firstMember, is(nullValue()));
	}

	@Test
	public void lpush_should_add_element_at_left_position() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		
		Long numberOfMembers = listDatatypeOperations.lpush(GROUP_NAME, DRUMER, KEYBOARD);
		assertThat(numberOfMembers, is(5L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(KEYBOARD), wrap(DRUMER), wrap(VOCALIST), wrap(GUITAR), wrap(BASSIST)));
		
	}
	
	@Test
	public void lpush_should_add_elements_at_empty_list() {
		
		Long numberOfMembers = listDatatypeOperations.lpush(GROUP_NAME, DRUMER, KEYBOARD);
		assertThat(numberOfMembers, is(2L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(KEYBOARD), wrap(DRUMER)));
		
	}
	
	@Test
	public void rpush_should_add_element_at_right_position() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		
		Long numberOfMembers = listDatatypeOperations.rpush(GROUP_NAME, DRUMER, KEYBOARD);
		assertThat(numberOfMembers, is(5L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(GUITAR), wrap(BASSIST), wrap(DRUMER), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void rpush_should_add_elements_at_empty_list() {
		
		Long numberOfMembers = listDatatypeOperations.rpush(GROUP_NAME, DRUMER, KEYBOARD);
		assertThat(numberOfMembers, is(2L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(DRUMER), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void lpushx_should_add_element_at_left_position_if_key_present() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		
		Long numberOfMembers = listDatatypeOperations.lpushx(GROUP_NAME, DRUMER);
		assertThat(numberOfMembers, is(4L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(DRUMER), wrap(VOCALIST), wrap(GUITAR), wrap(BASSIST)));
		
	}
	
	@Test
	public void lpushx_should_not_add_elements_if_key_not_present() {
		
		Long numberOfMembers = listDatatypeOperations.lpushx(GROUP_NAME, DRUMER);
		assertThat(numberOfMembers, is(0L));
		assertThat(listDatatypeOperations.blockingMultimap.size(wrap(GROUP_NAME)), is(0));
		
	}
	
	@Test
	public void rpushx_should_add_element_at_right_position_if_key_present() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		
		Long numberOfMembers = listDatatypeOperations.rpushx(GROUP_NAME, DRUMER);
		assertThat(numberOfMembers, is(4L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(GUITAR), wrap(BASSIST), wrap(DRUMER)));
		
	}
	
	@Test
	public void rpushx_should_not_add_elements_if_key_not_present() {
		
		Long numberOfMembers = listDatatypeOperations.rpushx(GROUP_NAME, DRUMER);
		assertThat(numberOfMembers, is(0L));
		
		assertThat(listDatatypeOperations.blockingMultimap.size(wrap(GROUP_NAME)), is(0));
		
	}
	
	@Test
	public void lrange_should_return_elements_between_inclusive_range() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(DRUMER));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		List<byte[]> elementsByRange = listDatatypeOperations.lrange(GROUP_NAME, 1, 3);
		assertThat(elementsByRange.size(), is(3));
		
		assertThat(elementsByRange, contains(GUITAR, BASSIST, DRUMER));
		
	}
	
	@Test
	public void lrange_should_return_elements_between_negative_inclusive_range() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(DRUMER));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		List<byte[]> elementsByRange = listDatatypeOperations.lrange(GROUP_NAME, -2, -1);
		assertThat(elementsByRange.size(), is(2));
		
		assertThat(elementsByRange, contains(DRUMER, KEYBOARD));
		
	}
	
	@Test
	public void lrange_should_treat_end_as_length_for_end_bigger_than_length() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(DRUMER));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		List<byte[]> elementsByRange = listDatatypeOperations.lrange(GROUP_NAME, -2, 10);
		assertThat(elementsByRange.size(), is(2));
		
		assertThat(elementsByRange, contains(DRUMER, KEYBOARD));
		
	}
	
	@Test
	public void lrange_should_return_empty_list_if_start_bigger_than_end() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(BASSIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(DRUMER));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		List<byte[]> elementsByRange = listDatatypeOperations.lrange(GROUP_NAME, 2, 1);
		assertThat(elementsByRange.size(), is(0));
		
	}
	
	@Test
	public void lrem_should_remove_max_number_of_given_elements() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		Long numberOfRemovedElements = listDatatypeOperations.lrem(GROUP_NAME, 5, VOCALIST);
		assertThat(numberOfRemovedElements, is(3L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(GUITAR), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void lrem_should_remove_all_elements_if_zero_count() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		Long numberOfRemovedElements = listDatatypeOperations.lrem(GROUP_NAME, 0, VOCALIST);
		assertThat(numberOfRemovedElements, is(3L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(GUITAR), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void lrem_should_remove_number_of_elements() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		Long numberOfRemovedElements = listDatatypeOperations.lrem(GROUP_NAME, 2, VOCALIST);
		assertThat(numberOfRemovedElements, is(2L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(GUITAR), wrap(VOCALIST), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void lrem_should_remove_number_of_elements_from_last() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		Long numberOfRemovedElements = listDatatypeOperations.lrem(GROUP_NAME, -2, VOCALIST);
		assertThat(numberOfRemovedElements, is(2L));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(GUITAR), wrap(KEYBOARD)));
		
	}
	
	@Test
	public void lset_should_add_element_at_given_index() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		
		String result = listDatatypeOperations.lset(GROUP_NAME, 1, BASSIST);
		assertThat(result, is("OK"));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(BASSIST)));
		
	}
	
	@Test
	public void lset_should_add_element_with_negative_index() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		
		String result = listDatatypeOperations.lset(GROUP_NAME, -1, BASSIST);
		assertThat(result, is("OK"));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(VOCALIST), wrap(BASSIST)));
		
	}
	
	@Test
	public void lset_should_return_error_if_index_out_of_bounds() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		
		String result = listDatatypeOperations.lset(GROUP_NAME, 4, BASSIST);
		assertThat(result, is("-"));
		
	}
	
	@Test
	public void ltrim_should_trim_elements_list() {
		
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(VOCALIST));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(GUITAR));
		listDatatypeOperations.blockingMultimap.put(wrap(GROUP_NAME), wrap(KEYBOARD));
		
		String result = listDatatypeOperations.ltrim(GROUP_NAME, 1, -1);
		assertThat(result, is("OK"));
		
		Collection<ByteBuffer> groupMembers = listDatatypeOperations.blockingMultimap.elements(wrap(GROUP_NAME));
		assertThat(groupMembers, contains(wrap(GUITAR), wrap(KEYBOARD)));
	}
	
}
