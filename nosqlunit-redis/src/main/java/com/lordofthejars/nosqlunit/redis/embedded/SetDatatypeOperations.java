package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.convert;
import static ch.lambdaj.collection.LambdaCollections.with;
import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SetDatatypeOperations extends ExpirationDatatypeOperations implements RedisDatatypeOperations {

	protected static final String SET = "set";

	private static final Random random = new Random();

	protected Multimap<ByteBuffer, ByteBuffer> setElements = HashMultimap.create();

	/**
	 * Add the specified member to the set value stored at key. If member is
	 * already a member of the set no operation is performed. If key does not
	 * exist a new set with the specified member as sole member is created. If
	 * the key exists but does not hold a set value an error is returned.
	 * <p>
	 * 
	 * @param key
	 * @param members
	 * @return Integer reply, specifically: 1 if the new element was added 0 if
	 *         the element was already a member of the set
	 */
	public Long sadd(final byte[] key, final byte[]... members) {

		long numberOfAddedElements = 0;

		for (byte[] bs : members) {
			if (setElements.put(wrap(key), wrap(bs))) {
				numberOfAddedElements++;
			}
		}

		return numberOfAddedElements;
	}

	/**
	 * Return the set cardinality (number of elements). If the key does not
	 * exist 0 is returned, like for empty sets.
	 * 
	 * @param key
	 * @return Integer reply, specifically: the cardinality (number of elements)
	 *         of the set as an integer.
	 */
	public Long scard(final byte[] key) {
		ByteBuffer keyBuffer = wrap(key);
		return (long) setElements.get(keyBuffer).size();
	}

	/**
	 * Return the difference between the Set stored at key1 and all the Sets
	 * key2, ..., keyN
	 * <p>
	 * <b>Example:</b>
	 * 
	 * <pre>
	 * key1 = [x, a, b, c]
	 * key2 = [c]
	 * key3 = [a, d]
	 * SDIFF key1,key2,key3 => [x, b]
	 * </pre>
	 * 
	 * Non existing keys are considered like empty sets.
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * 
	 * @param keys
	 * @return Return the members of a set resulting from the difference between
	 *         the first set provided and all the successive sets.
	 */
	public Set<byte[]> sdiff(final byte[]... keys) {

		Set<ByteBuffer> targetKey = differenceElements(keys);

		List<byte[]> diff = convert(targetKey, ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		return new HashSet<byte[]>(diff);

	}

	private Set<ByteBuffer> differenceElements(final byte[]... keys) {
		if (keys.length == 0) {
			return new HashSet<ByteBuffer>();
		}

		Set<ByteBuffer> targetKey = new HashSet<ByteBuffer>(getReferenceElement(keys));
		targetKey = removeElements(targetKey, keys);
		return targetKey;
	}

	private Set<ByteBuffer> removeElements(Set<ByteBuffer> targetKey, final byte[]... keys) {
		for (int index = 1; index < keys.length; index++) {
			Collection<ByteBuffer> collectionElements = setElements.get(wrap(keys[index]));
			targetKey.removeAll(collectionElements);
		}

		return targetKey;
	}

	/**
	 * This command works exactly like {@link #sdiff(String...) SDIFF} but
	 * instead of being returned the resulting set is stored in dstkey.
	 * 
	 * @param dstkey
	 * @param keys
	 * @return Status code reply
	 */
	public Long sdiffstore(final byte[] dstkey, final byte[]... keys) {

		Set<ByteBuffer> sdiff = differenceElements(keys);

		setElements.replaceValues(wrap(dstkey), sdiff);

		return (long) sdiff.size();
	}

	private void removeAllValues(final byte[] dstkey) {
		setElements.removeAll(wrap(dstkey));
	}

	/**
	 * Return the members of a set resulting from the intersection of all the
	 * sets hold at the specified keys. Like in
	 * {@link #lrange(byte[], int, int) LRANGE} the result is sent to the client
	 * as a multi-bulk reply (see the protocol specification for more
	 * information). If just a single key is specified, then this command
	 * produces the same result as {@link #smembers(byte[]) SMEMBERS}. Actually
	 * SMEMBERS is just syntax sugar for SINTER.
	 * <p>
	 * Non existing keys are considered like empty sets, so if one of the keys
	 * is missing an empty set is returned (since the intersection with an empty
	 * set always is an empty set).
	 * <p>
	 * 
	 * @param keys
	 * @return Multi bulk reply, specifically the list of common elements.
	 */
	public Set<byte[]> sinter(final byte[]... keys) {

		Set<ByteBuffer> targetKey = intersactionElements(keys);

		List<byte[]> intersaction = convert(targetKey, ByteBuffer2ByteArrayConverter.createByteBufferConverter());

		return new HashSet<byte[]>(intersaction);

	}

	private Set<ByteBuffer> intersactionElements(final byte[]... keys) {

		if (keys.length == 0) {
			return new HashSet<ByteBuffer>();
		}

		Set<ByteBuffer> targetKey = new HashSet<ByteBuffer>(getReferenceElement(keys));
		targetKey = retainElements(targetKey, keys);
		return targetKey;
	}

	private Set<ByteBuffer> retainElements(Set<ByteBuffer> targetKey, final byte[]... keys) {
		for (int index = 1; index < keys.length; index++) {
			Collection<ByteBuffer> collectionElements = setElements.get(wrap(keys[index]));
			targetKey.retainAll(collectionElements);
		}

		return targetKey;
	}

	/**
	 * This commnad works exactly like {@link #sinter(String...) SINTER} but
	 * instead of being returned the resulting set is sotred as dstkey.
	 * <p>
	 * 
	 * @param dstkey
	 * @param keys
	 * @return Status code reply
	 */
	public Long sinterstore(final byte[] dstkey, final byte[]... keys) {

		Set<ByteBuffer> sintersect = intersactionElements(keys);

		setElements.replaceValues(wrap(dstkey), sintersect);

		return (long) sintersect.size();

	}

	/**
	 * Return 1 if member is a member of the set stored at key, otherwise 0 is
	 * returned.
	 * <p>
	 * 
	 * @param key
	 * @param member
	 * @return Integer reply, specifically: 1 if the element is a member of the
	 *         set 0 if the element is not a member of the set OR if the key
	 *         does not exist
	 */
	public Boolean sismember(final byte[] key, final byte[] member) {
		Collection<ByteBuffer> elements = setElements.get(wrap(key));
		return with(elements).contains(wrap(member));
	}

	/**
	 * Return all the members (elements) of the set value stored at key. This is
	 * just syntax glue for {@link #sinter(String...) SINTER}.
	 * <p>
	 * 
	 * @param key
	 * @return Multi bulk reply
	 */
	public Set<byte[]> smembers(final byte[] key) {

		Collection<ByteBuffer> elements = setElements.get(wrap(key));

		List<byte[]> allElements = convert(elements, ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		return new HashSet<byte[]>(allElements);
	}

	/**
	 * Move the specified member from the set at srckey to the set at dstkey.
	 * This operation is atomic, in every given moment the element will appear
	 * to be in the source or destination set for accessing clients.
	 * <p>
	 * If the source set does not exist or does not contain the specified
	 * element no operation is performed and zero is returned, otherwise the
	 * element is removed from the source set and added to the destination set.
	 * On success one is returned, even if the element was already present in
	 * the destination set.
	 * <p>
	 * An error is raised if the source or destination keys contain a non Set
	 * value.
	 * <p>
	 * 
	 * @param srckey
	 * @param dstkey
	 * @param member
	 * @return Integer reply, specifically: 1 if the element was moved 0 if the
	 *         element was not found on the first set and no operation was
	 *         performed
	 */
	public Long smove(final byte[] srckey, final byte[] dstkey, final byte[] member) {

		boolean isElementPresent = sismember(srckey, member);

		if (isElementPresent) {

			removeElement(srckey, member);
			sadd(dstkey, member);

			return 1L;
		}

		return 0L;

	}

	/**
	 * Remove a random element from a Set returning it as return value. If the
	 * Set is empty or the key does not exist, a nil object is returned.
	 * <p>
	 * The {@link #srandmember(byte[])} command does a similar work but the
	 * returned element is not removed from the Set.
	 * <p>
	 * 
	 * @param key
	 * @return Bulk reply
	 */
	public byte[] spop(final byte[] key) {

		byte[] removedElement = srandmember(key);

		if (removedElement != null) {
			return removeElement(key, removedElement);
		}

		return null;
	}

	/**
	 * Return a random element from a Set, without removing the element. If the
	 * Set is empty or the key does not exist, a nil object is returned.
	 * <p>
	 * The SPOP command does a similar work but the returned element is popped
	 * (removed) from the Set.
	 * <p>
	 * 
	 * @param key
	 * @return Bulk reply
	 */
	public byte[] srandmember(final byte[] key) {

		Set<byte[]> elements = smembers(key);

		if (elements.size() > 0) {
			int randomPosition = randomInteger(elements.size());
			return elementInPosition(elements, randomPosition);
		}

		return null;
	}

	/**
	 * Remove the specified member from the set value stored at key. If member
	 * was not a member of the set no operation is performed. If key does not
	 * hold a set value an error is returned.
	 * <p>
	 * 
	 * @param key
	 * @param member
	 * @return Integer reply, specifically: 1 if the new element was removed 0
	 *         if the new element was not a member of the set
	 */
	public Long srem(final byte[] key, final byte[]... members) {

		long numberOfRemovedElements = 0L;

		for (byte[] member : members) {
			if (removeElement(key, member) != null) {
				numberOfRemovedElements++;
			}
		}

		return numberOfRemovedElements;
	}

	/**
	 * Return the members of a set resulting from the union of all the sets hold
	 * at the specified keys. Like in {@link #lrange(byte[], int, int) LRANGE}
	 * the result is sent to the client as a multi-bulk reply (see the protocol
	 * specification for more information). If just a single key is specified,
	 * then this command produces the same result as {@link #smembers(byte[])
	 * SMEMBERS}.
	 * <p>
	 * Non existing keys are considered like empty sets.
	 * <p>
	 * 
	 * @param keys
	 * @return Multi bulk reply, specifically the list of common elements.
	 */
	public Set<byte[]> sunion(final byte[]... keys) {

		Set<ByteBuffer> unionElements = unionElements(keys);

		List<byte[]> allElements = convert(unionElements, ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		return new HashSet<byte[]>(allElements);
	}

	/**
	 * This command works exactly like {@link #sunion(String...) SUNION} but
	 * instead of being returned the resulting set is stored as dstkey. Any
	 * existing value in dstkey will be over-written.
	 * <p>
	 * 
	 * @param dstkey
	 * @param keys
	 * @return Status code reply
	 */
	public Long sunionstore(final byte[] dstkey, final byte[]... keys) {

		Set<ByteBuffer> sunion = unionElements(keys);
		setElements.replaceValues(wrap(dstkey), sunion);

		return (long) sunion.size();
	}

	public long getNumberOfKeys() {
		return this.setElements.keySet().size();
	}

	public void flushAllKeys() {
		this.setElements.clear();
	}

	private Collection<ByteBuffer> getReferenceElement(final byte[]... keys) {
		return setElements.get(wrap(keys[0]));
	}

	private Set<ByteBuffer> unionElements(final byte[]... keys) {
		Set<ByteBuffer> unionElements = new HashSet<ByteBuffer>();

		for (byte[] key : keys) {
			unionElements.addAll(setElements.get(wrap(key)));
		}

		return unionElements;
	}

	private byte[] removeElement(final byte[] srckey, final byte[] member) {
		Collection<ByteBuffer> elements = setElements.get(wrap(srckey));
		boolean isRemoved = elements.remove(wrap(member));

		return isRemoved ? member : null;

	}

	private int randomInteger(int n) {
		return random.nextInt(n);
	}

	private byte[] elementInPosition(Collection<byte[]> elements, int position) {
		int currentPosition = 0;

		for (byte[] element : elements) {

			if (currentPosition == position) {
				return element;
			}

			currentPosition++;
		}

		return null;
	}

	@Override
	public Long del(byte[]... keys) {

		long numberOfRemovedElements = 0;

		for (byte[] key : keys) {
			ByteBuffer wrappedKey = wrap(key);
			if (this.setElements.containsKey(wrappedKey)) {
				this.setElements.removeAll(wrappedKey);
				removeExpiration(key);
				numberOfRemovedElements++;
			}
		}

		return numberOfRemovedElements;
	}

	@Override
	public boolean exists(byte[] key) {
		return this.setElements.containsKey(wrap(key));
	}

	@Override
	public boolean renameKey(byte[] key, byte[] newKey) {
		ByteBuffer wrappedKey = wrap(key);

		if (this.setElements.containsKey(wrappedKey)) {
			Collection<ByteBuffer> elements = this.setElements.get(wrappedKey);
			this.setElements.removeAll(wrap(newKey));
			this.setElements.putAll(wrap(newKey), elements);
			this.setElements.removeAll(wrappedKey);
			
			renameTtlKey(key, newKey);
			
			return true;
		}

		return false;
	}

	@Override
	public List<byte[]> keys() {
		return new ArrayList<byte[]>(convert(this.setElements.keySet(),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	@Override
	public String type() {
		return SET;
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		try {
			return sortNumberValues(key);
		} catch (NumberFormatException e) {
			return convert(this.setElements.get(wrap(key)),
					ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		}
	}

	
	private List<byte[]> sortNumberValues(byte[] key) {
		Collection<ByteBuffer> elements = this.setElements.get(wrap(key));
		List<Double> values = convert(elements, ByteBufferAsString2DoubleConverter.createByteBufferAsStringToDoubleConverter());
		
		Collections.sort(values);
		return new LinkedList<byte[]>(convert(values,
				DoubleToStringByteArrayConverter.createDoubleToStringByteArrayConverter()));
	}
	
}
