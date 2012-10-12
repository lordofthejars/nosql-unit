package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.convert;
import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ListDatatypeOperations extends ExpirationDatatypeOperations implements RedisDatatypeOperations {

	protected static final String LIST = "list";
	private static final String KO = "-";
	private static final String OK = "OK";
	protected BlockingMap<ByteBuffer, ByteBuffer> blockingMultimap = TransferMap.create();

	public enum ListPositionEnum {
		BEFORE, AFTER;
	}

	/**
	 * BLPOP (and BRPOP) is a blocking list pop primitive. You can see this
	 * commands as blocking versions of LPOP and RPOP able to block if the
	 * specified keys don't exist or contain empty lists.
	 * <p>
	 * The following is a description of the exact semantic. We describe BLPOP
	 * but the two commands are identical, the only difference is that BLPOP
	 * pops the element from the left (head) of the list, and BRPOP pops from
	 * the right (tail).
	 * <p>
	 * <b>Non blocking behavior</b>
	 * <p>
	 * When BLPOP is called, if at least one of the specified keys contain a non
	 * empty list, an element is popped from the head of the list and returned
	 * to the caller together with the name of the key (BLPOP returns a two
	 * elements array, the first element is the key, the second the popped
	 * value).
	 * <p>
	 * Keys are scanned from left to right, so for instance if you issue BLPOP
	 * list1 list2 list3 0 against a dataset where list1 does not exist but
	 * list2 and list3 contain non empty lists, BLPOP guarantees to return an
	 * element from the list stored at list2 (since it is the first non empty
	 * list starting from the left).
	 * <p>
	 * <b>Blocking behavior</b>
	 * <p>
	 * If none of the specified keys exist or contain non empty lists, BLPOP
	 * blocks until some other client performs a LPUSH or an RPUSH operation
	 * against one of the lists.
	 * <p>
	 * Once new data is present on one of the lists, the client finally returns
	 * with the name of the key unblocking it and the popped value.
	 * <p>
	 * When blocking, if a non-zero timeout is specified, the client will
	 * unblock returning a nil special value if the specified amount of seconds
	 * passed without a push operation against at least one of the specified
	 * keys.
	 * <p>
	 * The timeout argument is interpreted as an integer value. A timeout of
	 * zero means instead to block forever.
	 * <p>
	 * <b>Multiple clients blocking for the same keys</b>
	 * <p>
	 * Multiple clients can block for the same key. They are put into a queue,
	 * so the first to be served will be the one that started to wait earlier,
	 * in a first-blpopping first-served fashion.
	 * <p>
	 * <b>blocking POP inside a MULTI/EXEC transaction</b>
	 * <p>
	 * BLPOP and BRPOP can be used with pipelining (sending multiple commands
	 * and reading the replies in batch), but it does not make sense to use
	 * BLPOP or BRPOP inside a MULTI/EXEC block (a Redis transaction).
	 * <p>
	 * The behavior of BLPOP inside MULTI/EXEC when the list is empty is to
	 * return a multi-bulk nil reply, exactly what happens when the timeout is
	 * reached. If you like science fiction, think at it like if inside
	 * MULTI/EXEC the time will flow at infinite speed :)
	 * <p>
	 * 
	 * @see #brpop(int, String...)
	 * 
	 * @param timeout
	 * @param keys
	 * @return BLPOP returns a two-elements array via a multi bulk reply in
	 *         order to return both the unblocking key and the popped value.
	 *         <p>
	 *         When a non-zero timeout is specified, and the BLPOP operation
	 *         timed out, the return value is a nil multi bulk reply. Most
	 *         client values will return false or nil accordingly to the
	 *         programming language used.
	 */
	public List<byte[]> blpop(final int timeout, final byte[]... keys) {

		ExecutorService executorService = Executors.newCachedThreadPool();
		final CountDownLatch countDownLatch = new CountDownLatch(1);

		@SuppressWarnings("unchecked")
		List<Callable<KeyMembers>> popActions = Collections.EMPTY_LIST;

		if (timeout == 0) {
			popActions = addFuturePopWithoutTimeout(countDownLatch, keys);
		} else {
			popActions = addFuturePopWithTimeout(countDownLatch, timeout, keys);
		}

		return executeFutureTasksWaitingForData(executorService, countDownLatch, popActions);

	}

	/**
	 * Pop a value from a list, push it to another list and return it; or block
	 * until one is available
	 * 
	 * @param source
	 * @param destination
	 * @param timeout
	 * @return the element
	 */
	public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {

		List<byte[]> element = brpop(timeout, source);

		if (element != null) {
			lpush(destination, element.get(1));
			return element.get(1);
		}

		return null;
	}

	public Long linsert(final byte[] key, final ListPositionEnum where, final byte[] pivot, final byte[] value) {

		ByteBuffer wrappedKey = wrap(key);

		if (this.blockingMultimap.containsKey(wrappedKey)) {

			int index = this.blockingMultimap.indexOf(wrappedKey, wrap(pivot));

			if (isPivotFound(index)) {
				this.blockingMultimap.addElementAt(wrappedKey, wrap(value), calculateIndexPosition(index, where));
				return (long) this.blockingMultimap.size(wrappedKey);
			}
			return -1L;
		} else {
			return 0L;
		}

	}

	/**
	 * Atomically return and remove the first (LPOP) or last (RPOP) element of
	 * the list. For example if the list contains the elements "a","b","c" LPOP
	 * will return "a" and the list will become "b","c".
	 * <p>
	 * If the key does not exist or the list is already empty the special value
	 * 'nil' is returned.
	 * 
	 * @see #rpop(byte[])
	 * 
	 * @param key
	 * @return Bulk reply
	 */
	public byte[] lpop(final byte[] key) {

		ByteBuffer polledElement = this.blockingMultimap.pollFirst(wrap(key));
		return polledElement == null ? null : polledElement.array();

	}

	/**
	 * Atomically return and remove the first (LPOP) or last (RPOP) element of
	 * the list. For example if the list contains the elements "a","b","c" LPOP
	 * will return "a" and the list will become "b","c".
	 * <p>
	 * If the key does not exist or the list is already empty the special value
	 * 'nil' is returned.
	 * 
	 * @see #lpop(byte[])
	 * 
	 * @param key
	 * @return Bulk reply
	 */
	public byte[] rpop(final byte[] key) {

		ByteBuffer polledElement = this.blockingMultimap.pollLast(wrap(key));
		return polledElement == null ? null : polledElement.array();
	}

	/**
	 * Return the length of the list stored at the specified key. If the key
	 * does not exist zero is returned (the same behaviour as for empty lists).
	 * If the value stored at key is not a list an error is returned.
	 * <p>
	 * 
	 * @param key
	 * @return The length of the list.
	 */
	public Long llen(final byte[] key) {
		return (long) this.blockingMultimap.size(wrap(key));
	}

	/**
	 * Set a new value as the element at index position of the List at key.
	 * <p>
	 * Out of range indexes will generate an error.
	 * <p>
	 * Similarly to other list commands accepting indexes, the index can be
	 * negative to access elements starting from the end of the list. So -1 is
	 * the last element, -2 is the penultimate, and so forth.
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * 
	 * @see #lindex(byte[], int)
	 * 
	 * @param key
	 * @param index
	 * @param value
	 * @return Status code reply
	 */
	public String lset(final byte[] key, final int index, final byte[] value) {

		ByteBuffer wrappedKey = wrap(key);
		long numberOfElements = this.blockingMultimap.size(wrappedKey);

		int realIndex = index;

		if (realIndex < 0) {
			realIndex = (int) (numberOfElements + index);
		}

		try {
			this.blockingMultimap.remove(wrappedKey, realIndex);
			ByteBuffer insertedValue = this.blockingMultimap.addElementAt(wrappedKey, wrap(value), realIndex);
			
			if(insertedValue == null) {
				return KO;
			}
			
		} catch (IndexOutOfBoundsException e) {
			return KO;
		}

		return OK;
	}

	private boolean isPivotFound(int index) {
		return index != -1;
	}

	private int calculateIndexPosition(int index, ListPositionEnum where) {
		switch (where) {
		case BEFORE:
			return index;
		case AFTER:
			return index + 1;
		default:
			return index;
		}
	}

	/**
	 * Return the specified element of the list stored at the specified key. 0
	 * is the first element, 1 the second and so on. Negative indexes are
	 * supported, for example -1 is the last element, -2 the penultimate and so
	 * on.
	 * <p>
	 * If the value stored at key is not of list type an error is returned. If
	 * the index is out of range a 'nil' reply is returned.
	 * <p>
	 * Note that even if the average time complexity is O(n) asking for the
	 * first or the last element of the list is O(1).
	 * <p>
	 * 
	 * @param key
	 * @param index
	 * @return Bulk reply, specifically the requested element
	 */
	public byte[] lindex(final byte[] key, final int index) {

		ByteBuffer elementAtIndex = this.blockingMultimap.getElement(wrap(key), index);
		return elementAtIndex == null ? null : elementAtIndex.array();

	}

	/**
	 * BLPOP (and BRPOP) is a blocking list pop primitive. You can see this
	 * commands as blocking versions of LPOP and RPOP able to block if the
	 * specified keys don't exist or contain empty lists.
	 * <p>
	 * The following is a description of the exact semantic. We describe BLPOP
	 * but the two commands are identical, the only difference is that BLPOP
	 * pops the element from the left (head) of the list, and BRPOP pops from
	 * the right (tail).
	 * <p>
	 * <b>Non blocking behavior</b>
	 * <p>
	 * When BLPOP is called, if at least one of the specified keys contain a non
	 * empty list, an element is popped from the head of the list and returned
	 * to the caller together with the name of the key (BLPOP returns a two
	 * elements array, the first element is the key, the second the popped
	 * value).
	 * <p>
	 * Keys are scanned from left to right, so for instance if you issue BLPOP
	 * list1 list2 list3 0 against a dataset where list1 does not exist but
	 * list2 and list3 contain non empty lists, BLPOP guarantees to return an
	 * element from the list stored at list2 (since it is the first non empty
	 * list starting from the left).
	 * <p>
	 * <b>Blocking behavior</b>
	 * <p>
	 * If none of the specified keys exist or contain non empty lists, BLPOP
	 * blocks until some other client performs a LPUSH or an RPUSH operation
	 * against one of the lists.
	 * <p>
	 * Once new data is present on one of the lists, the client finally returns
	 * with the name of the key unblocking it and the popped value.
	 * <p>
	 * When blocking, if a non-zero timeout is specified, the client will
	 * unblock returning a nil special value if the specified amount of seconds
	 * passed without a push operation against at least one of the specified
	 * keys.
	 * <p>
	 * The timeout argument is interpreted as an integer value. A timeout of
	 * zero means instead to block forever.
	 * <p>
	 * <b>Multiple clients blocking for the same keys</b>
	 * <p>
	 * Multiple clients can block for the same key. They are put into a queue,
	 * so the first to be served will be the one that started to wait earlier,
	 * in a first-blpopping first-served fashion.
	 * <p>
	 * <b>blocking POP inside a MULTI/EXEC transaction</b>
	 * <p>
	 * BLPOP and BRPOP can be used with pipelining (sending multiple commands
	 * and reading the replies in batch), but it does not make sense to use
	 * BLPOP or BRPOP inside a MULTI/EXEC block (a Redis transaction).
	 * <p>
	 * The behavior of BLPOP inside MULTI/EXEC when the list is empty is to
	 * return a multi-bulk nil reply, exactly what happens when the timeout is
	 * reached. If you like science fiction, think at it like if inside
	 * MULTI/EXEC the time will flow at infinite speed :)
	 * <p>
	 * 
	 * @see #blpop(int, String...)
	 * 
	 * @param timeout
	 * @param keys
	 * @return BLPOP returns a two-elements array via a multi bulk reply in
	 *         order to return both the unblocking key and the popped value.
	 *         <p>
	 *         When a non-zero timeout is specified, and the BLPOP operation
	 *         timed out, the return value is a nil multi bulk reply. Most
	 *         client values will return false or nil accordingly to the
	 *         programming language used.
	 */
	public List<byte[]> brpop(final int timeout, final byte[]... keys) {

		ExecutorService executorService = Executors.newCachedThreadPool();
		final CountDownLatch countDownLatch = new CountDownLatch(1);

		@SuppressWarnings("unchecked")
		List<Callable<KeyMembers>> popActions = Collections.EMPTY_LIST;

		if (timeout == 0) {
			popActions = addFutureLastPopWithoutTimeout(countDownLatch, keys);
		} else {
			popActions = addFutureLastPopWithTimeout(countDownLatch, timeout, keys);
		}

		return executeFutureTasksWaitingForData(executorService, countDownLatch, popActions);

	}

	/**
	 * Add the string value to the head (LPUSH) or tail (RPUSH) of the list
	 * stored at key. If the key does not exist an empty list is created just
	 * before the append operation. If the key exists but is not a List an error
	 * is returned.
	 * <p>
	 * 
	 * @see BinaryJedis#rpush(byte[], byte[]...)
	 * 
	 * @param key
	 * @param elements
	 * @return Integer reply, specifically, the number of elements inside the
	 *         list after the push operation.
	 */
	public Long lpush(final byte[] key, final byte[]... values) {

		Collection<ByteBuffer> elements = convert(values, new ByteArray2ByteBufferConverter());

		this.blockingMultimap.putFirst(wrap(key), elements);
		return (long) blockingMultimap.size(wrap(key));

	}

	public Long lpushx(final byte[] key, final byte[] value) {
		if (this.blockingMultimap.containsKey(wrap(key))) {
			return lpush(key, value);
		}
		return (long) this.blockingMultimap.size(wrap(key));
	}

	public Long rpushx(final byte[] key, final byte[] value) {

		if (this.blockingMultimap.containsKey(wrap(key))) {
			return rpush(key, value);
		}
		return (long) this.blockingMultimap.size(wrap(key));

	}

	/**
	 * Return the specified elements of the list stored at the specified key.
	 * Start and end are zero-based indexes. 0 is the first element of the list
	 * (the list head), 1 the next element and so on.
	 * <p>
	 * For example LRANGE foobar 0 2 will return the first three elements of the
	 * list.
	 * <p>
	 * start and end can also be negative numbers indicating offsets from the
	 * end of the list. For example -1 is the last element of the list, -2 the
	 * penultimate element and so on.
	 * <p>
	 * <b>Consistency with range functions in various programming languages</b>
	 * <p>
	 * Note that if you have a list of numbers from 0 to 100, LRANGE 0 10 will
	 * return 11 elements, that is, rightmost item is included. This may or may
	 * not be consistent with behavior of range-related functions in your
	 * programming language of choice (think Ruby's Range.new, Array#slice or
	 * Python's range() function).
	 * <p>
	 * LRANGE behavior is consistent with one of Tcl.
	 * <p>
	 * <b>Out-of-range indexes</b>
	 * <p>
	 * Indexes out of range will not produce an error: if start is over the end
	 * of the list, or start > end, an empty list is returned. If end is over
	 * the end of the list Redis will threat it just like the last element of
	 * the list.
	 * <p>
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return Multi bulk reply, specifically a list of elements in the
	 *         specified range.
	 */
	public List<byte[]> lrange(final byte[] key, final int start, final int end) {

		List<ByteBuffer> elements = new LinkedList<ByteBuffer>(this.blockingMultimap.elements(wrap(key)));

		int calculatedStart = RangeUtils.calculateStart(start, elements.size());
		int calculatedEnd = RangeUtils.calculateEnd(end, elements.size());

		try {
			List<ByteBuffer> subList = elements.subList(calculatedStart, calculatedEnd);
			return convert(subList, new ByteBuffer2ByteArrayConverter());

		} catch (IndexOutOfBoundsException e) {
			return Collections.EMPTY_LIST;
		}

	}

	/**
	 * Add the string value to the head (LPUSH) or tail (RPUSH) of the list
	 * stored at key. If the key does not exist an empty list is created just
	 * before the append operation. If the key exists but is not a List an error
	 * is returned.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @see BinaryJedis#rpush(byte[], byte[]...)
	 * 
	 * @param key
	 * @param elements
	 * @return Integer reply, specifically, the number of elements inside the
	 *         list after the push operation.
	 */
	public Long rpush(final byte[] key, final byte[]... values) {

		Collection<ByteBuffer> elements = convert(values, new ByteArray2ByteBufferConverter());

		this.blockingMultimap.putLast(wrap(key), elements);
		return (long) blockingMultimap.size(wrap(key));
	}

	/**
	 * Remove the first count occurrences of the value element from the list. If
	 * count is zero all the elements are removed. If count is negative elements
	 * are removed from tail to head, instead to go from head to tail that is
	 * the normal behaviour. So for example LREM with count -2 and hello as
	 * value to remove against the list (a,b,c,hello,x,hello,hello) will have
	 * the list (a,b,c,hello,x). The number of removed elements is returned as
	 * an integer, see below for more information about the returned value. Note
	 * that non existing keys are considered like empty lists by LREM, so LREM
	 * against non existing keys will always return 0.
	 * <p>
	 * Time complexity: O(N) (with N being the length of the list)
	 * 
	 * @param key
	 * @param count
	 * @param value
	 * @return Integer Reply, specifically: The number of removed elements if
	 *         the operation succeeded
	 */
	public Long lrem(final byte[] key, final int count, final byte[] value) {

		ByteBuffer wrappedKey = wrap(key);
		ByteBuffer wrappedValue = wrap(value);

		long numberOfElementsRemoved = 0;

		if (count < 0) {
			numberOfElementsRemoved = removeLastElements(count, wrappedKey, wrappedValue);
		} else {
			if (count == 0) {
				numberOfElementsRemoved = removeAllElements(wrappedKey, wrappedValue);
			} else {
				numberOfElementsRemoved = removeFirstElements(count, wrappedKey, wrappedValue);
			}
		}

		return numberOfElementsRemoved;
	}

	/**
	 * Trim an existing list so that it will contain only the specified range of
	 * elements specified. Start and end are zero-based indexes. 0 is the first
	 * element of the list (the list head), 1 the next element and so on.
	 * <p>
	 * For example LTRIM foobar 0 2 will modify the list stored at foobar key so
	 * that only the first three elements of the list will remain.
	 * <p>
	 * start and end can also be negative numbers indicating offsets from the
	 * end of the list. For example -1 is the last element of the list, -2 the
	 * penultimate element and so on.
	 * <p>
	 * Indexes out of range will not produce an error: if start is over the end
	 * of the list, or start > end, an empty list is left as value. If end over
	 * the end of the list Redis will threat it just like the last element of
	 * the list.
	 * <p>
	 * Hint: the obvious use of LTRIM is together with LPUSH/RPUSH. For example:
	 * <p>
	 * {@code lpush("mylist", "someelement"); ltrim("mylist", 0, 99); * }
	 * <p>
	 * The above two commands will push elements in the list taking care that
	 * the list will not grow without limits. This is very useful when using
	 * Redis to store logs for example. It is important to note that when used
	 * in this way LTRIM is an O(1) operation because in the average case just
	 * one element is removed from the tail of the list.
	 * <p>
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return Status code reply
	 */
	public String ltrim(final byte[] key, final int start, final int end) {

		ByteBuffer wrappedKey = wrap(key);
		List<ByteBuffer> elements = new LinkedList<ByteBuffer>(this.blockingMultimap.elements(wrappedKey));

		int calculatedStart = RangeUtils.calculateStart(start, elements.size());
		int calculatedEnd = RangeUtils.calculateEnd(end, elements.size());

		try {
			List<ByteBuffer> sublist = elements.subList(calculatedStart, calculatedEnd);
			this.blockingMultimap.replaceValues(wrappedKey, sublist);
		} catch (IndexOutOfBoundsException e) {
			return KO;
		} catch(IllegalArgumentException e) {
			return KO;
		}

		return OK;

	}

	public long getNumberOfKeys() {
		return this.blockingMultimap.size();
	}

	public void flushAllKeys() {
		removeExpirations();
		this.blockingMultimap.clear();
	}

	private void removeExpirations() {
		List<byte[]> keys = this.keys();
		for (byte[] key : keys) {
			this.removeExpiration(key);
		}
	}
	
	private long removeFirstElements(final int count, ByteBuffer wrappedKey, ByteBuffer wrappedValue) {

		long numberOfElementsRemoved = 0;

		int numberOfElements = numberOfElementsToRemove(count);

		while (numberOfElements > 0) {
			int indexOf = this.blockingMultimap.indexOf(wrappedKey, wrappedValue);

			if (indexOf == -1) {
				break;
			}

			this.blockingMultimap.remove(wrappedKey, indexOf);
			numberOfElementsRemoved++;
			numberOfElements--;
		}

		return numberOfElementsRemoved;

	}

	private long removeAllElements(ByteBuffer wrappedKey, ByteBuffer wrappedValue) {

		long numberOfElementsRemoved = 0;
		int indexOf = this.blockingMultimap.indexOf(wrappedKey, wrappedValue);

		while (indexOf != -1) {
			this.blockingMultimap.remove(wrappedKey, indexOf);
			numberOfElementsRemoved++;
			indexOf = this.blockingMultimap.indexOf(wrappedKey, wrappedValue);
		}

		return numberOfElementsRemoved;
	}

	private long removeLastElements(final int count, ByteBuffer wrappedKey, ByteBuffer wrappedValue) {

		long numberOfElementsRemoved = 0;
		int numberOfElements = numberOfElementsToRemove(count);

		while (numberOfElements > 0) {
			int indexOf = this.blockingMultimap.lastIndexOf(wrappedKey, wrappedValue);

			if (indexOf == -1) {
				break;
			}

			this.blockingMultimap.remove(wrappedKey, indexOf);
			numberOfElementsRemoved++;
			numberOfElements--;

		}

		return numberOfElementsRemoved;
	}

	private int numberOfElementsToRemove(int count) {
		return Math.abs(count);
	}

	private List<byte[]> executeFutureTasksWaitingForData(ExecutorService executorService,
			final CountDownLatch countDownLatch, List<Callable<KeyMembers>> popActions) {
		List<Future<KeyMembers>> futures = new LinkedList<Future<KeyMembers>>();

		for (Callable<KeyMembers> callable : popActions) {
			futures.add(executorService.submit(callable));
		}

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			return null;
		}

		executorService.shutdownNow();

		try {
			Future<KeyMembers> futureWithValidKey = findFirstFutureWithValidData(futures);

			if (futureWithValidKey != null) {
				KeyMembers elements = futureWithValidKey.get();
				ByteBuffer membersOfKey = elements.getValue();

				if (membersOfKey != null) {

					List<byte[]> keyMember = new ArrayList<byte[]>();
					keyMember.add(elements.getKey().array());
					keyMember.add(membersOfKey.array());

					return keyMember;
				} else {
					return null;
				}
			}

		} catch (InterruptedException e) {
			return null;
		} catch (ExecutionException e) {
			return null;
		}

		return null;
	}

	private Future<KeyMembers> findFirstFutureWithValidData(List<Future<KeyMembers>> futures)
			throws InterruptedException, ExecutionException {

		for (Future<KeyMembers> future : futures) {
			if (future != null && future.get().getValue() != null) {
				return future;
			}
		}

		return null;
	}

	private List<Callable<KeyMembers>> addFutureLastPopWithoutTimeout(final CountDownLatch countDownLatch,
			final byte[]... keys) {
		final List<Callable<KeyMembers>> futureTasks = new LinkedList<Callable<KeyMembers>>();
		for (final byte[] key : keys) {

			futureTasks.add(new Callable<KeyMembers>() {

				public KeyMembers call() throws Exception {
					ByteBuffer result = blockingMultimap.lastAndWait(wrap(key));
					countDownLatch.countDown();

					KeyMembers keyMembers = new KeyMembers(wrap(key), result);
					return keyMembers;
				}
			});
		}
		return futureTasks;
	}

	private List<Callable<KeyMembers>> addFutureLastPopWithTimeout(final CountDownLatch countDownLatch,
			final int timeout, final byte[]... keys) {
		final List<Callable<KeyMembers>> futureTasks = new LinkedList<Callable<KeyMembers>>();
		for (final byte[] key : keys) {

			futureTasks.add(new Callable<KeyMembers>() {

				public KeyMembers call() throws Exception {
					ByteBuffer result = blockingMultimap.lastAndWait(wrap(key), timeout);

					countDownLatch.countDown();

					KeyMembers keyMembers = new KeyMembers(wrap(key), result);
					return keyMembers;
				}
			});
		}
		return futureTasks;
	}

	private List<Callable<KeyMembers>> addFuturePopWithoutTimeout(final CountDownLatch countDownLatch,
			final byte[]... keys) {
		final List<Callable<KeyMembers>> futureTasks = new LinkedList<Callable<KeyMembers>>();
		for (final byte[] key : keys) {

			futureTasks.add(new Callable<KeyMembers>() {

				public KeyMembers call() throws Exception {
					ByteBuffer result = blockingMultimap.getAndWait(wrap(key));
					countDownLatch.countDown();

					KeyMembers keyMembers = new KeyMembers(wrap(key), result);
					return keyMembers;
				}
			});
		}
		return futureTasks;
	}

	private List<Callable<KeyMembers>> addFuturePopWithTimeout(final CountDownLatch countDownLatch, final int timeout,
			final byte[]... keys) {
		final List<Callable<KeyMembers>> futureTasks = new LinkedList<Callable<KeyMembers>>();
		for (final byte[] key : keys) {

			futureTasks.add(new Callable<KeyMembers>() {

				public KeyMembers call() throws Exception {
					ByteBuffer result = blockingMultimap.getAndWait(wrap(key), timeout);

					countDownLatch.countDown();

					KeyMembers keyMembers = new KeyMembers(wrap(key), result);
					return keyMembers;
				}
			});
		}
		return futureTasks;
	}

	private final class KeyMembers {

		private ByteBuffer key;
		private ByteBuffer value;

		public KeyMembers(ByteBuffer key, ByteBuffer value) {
			super();
			this.key = key;
			this.value = value;
		}

		public ByteBuffer getKey() {
			return key;
		}

		public ByteBuffer getValue() {
			return value;
		}

	}

	@Override
	public Long del(byte[]... keys) {

		long numberOfRemovedElements = 0;

		for (byte[] key : keys) {
			ByteBuffer wrappedKey = wrap(key);
			if (this.blockingMultimap.containsKey(wrappedKey)) {
				this.blockingMultimap.clear(wrappedKey);
				removeExpiration(key);
				numberOfRemovedElements++;
			}
		}

		return numberOfRemovedElements;
	}

	@Override
	public boolean exists(byte[] key) {
		return this.blockingMultimap.containsKey(wrap(key));
	}

	@Override
	public boolean renameKey(byte[] key, byte[] newKey) {
		ByteBuffer wrappedKey = wrap(key);

		if (this.blockingMultimap.containsKey(wrappedKey)) {
			Collection<ByteBuffer> elements = this.blockingMultimap.elements(wrappedKey);
			this.blockingMultimap.clear(wrap(newKey));
			this.blockingMultimap.putLast(wrap(newKey), elements);
			this.blockingMultimap.clear(wrappedKey);
			
			renameTtlKey(key, newKey);
			
			return true;
		}

		return false;
	}

	@Override
	public List<byte[]> keys() {
		return new ArrayList<byte[]>(convert(this.blockingMultimap.keySet(),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	@Override
	public String type() {
		return LIST;
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		try {
			return sortNumberValues(key);
		} catch (NumberFormatException e) {
			return convert(this.blockingMultimap.elements(wrap(key)),
					ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		}
	}

	private List<byte[]> sortNumberValues(byte[] key) {
		List<Double> values = new ArrayList<Double>(convert(this.blockingMultimap.elements(wrap(key)),
				ByteBufferAsString2DoubleConverter.createByteBufferAsStringToDoubleConverter()));

		Collections.sort(values);
		return new LinkedList<byte[]>(convert(values,
				DoubleToStringByteArrayConverter.createDoubleToStringByteArrayConverter()));
	}

}
