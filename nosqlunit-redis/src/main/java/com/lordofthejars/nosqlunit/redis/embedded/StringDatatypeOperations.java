package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.convert;
import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import redis.clients.util.SafeEncoder;

public class StringDatatypeOperations extends ExpirationDatatypeOperations implements RedisDatatypeOperations {

	protected static final String STRING = "string";
	private static final int NEGATE = -1;
	private static final long NONE_SUCCESS = 0;
	private static final long SUCCESS = 1;
	private static final String OK = "OK";
	private static final ByteBuffer ZERO = wrap("0".getBytes());

	protected Map<ByteBuffer, ByteBuffer> simpleTypes = new HashMap<ByteBuffer, ByteBuffer>();

	/**
	 * If the key already exists and is a string, this command appends the
	 * provided value at the end of the string. If the key does not exist it is
	 * created and set as an empty string, so APPEND will be very similar to SET
	 * in this special case.
	 * <p>
	 * Time complexity: O(1). The amortized time complexity is O(1) assuming the
	 * appended value is small and the already present value is of any size,
	 * since the dynamic string library used by Redis will double the free space
	 * available on every reallocation.
	 * 
	 * @param key
	 * @param value
	 * @return Integer reply, specifically the total length of the string after
	 *         the append operation.
	 */
	public Long append(final byte[] key, final byte[] value) {

		ByteBuffer wrappedKey = wrap(key);

		if (simpleTypes.containsKey(wrappedKey)) {
			byte[] oldValue = simpleTypes.get(wrappedKey).array();
			byte[] newValue = RangeUtils.concat(oldValue, value);
			simpleTypes.put(wrappedKey, wrap(newValue));

			return (long) newValue.length;

		} else {
			set(key, value);
			return (long) value.length;
		}

	}

	/**
	 * Decrement the number stored at key by one. If the key does not exist or
	 * contains a value of a wrong type, set the key to the value of "0" before
	 * to perform the decrement operation.
	 * <p>
	 * INCR commands are limited to 64 bit signed integers.
	 * <p>
	 * Note: this is actually a string operation, that is, in Redis there are
	 * not "integer" types. Simply the string stored at the key is parsed as a
	 * base 10 64 bit signed integer, incremented, and then converted back as a
	 * string.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @see #incr(byte[])
	 * @see #incrBy(byte[], long)
	 * @see #decrBy(byte[], long)
	 * 
	 * @param key
	 * @return Integer reply, this commands will reply with the new value of key
	 *         after the increment.
	 */
	public Long decr(final byte[] key) {
		return decrBy(key, 1);
	}

	/**
	 * IDECRBY work just like {@link #decr(String) INCR} but instead to
	 * decrement by 1 the decrement is integer.
	 * <p>
	 * INCR commands are limited to 64 bit signed integers.
	 * <p>
	 * Note: this is actually a string operation, that is, in Redis there are
	 * not "integer" types. Simply the string stored at the key is parsed as a
	 * base 10 64 bit signed integer, incremented, and then converted back as a
	 * string.
	 * <p>
	 * 
	 * @see #incr(byte[])
	 * @see #decr(byte[])
	 * @see #incrBy(byte[], long)
	 * 
	 * @param key
	 * @param integer
	 * @return Integer reply, this commands will reply with the new value of key
	 *         after the increment.
	 */
	public Long decrBy(final byte[] key, final long integer) {

		long value = invertSign(integer);

		if (simpleTypes.containsKey(wrap(key))) {
			return incrementAndSetValue(key, value);
		} else {
			simpleTypes.put(wrap(key), ZERO);
			return incrementAndSetValue(key, value);
		}
	}

	private long invertSign(final long integer) {
		long value = integer * NEGATE;
		return value;
	}

	/**
	 * INCRBY work just like {@link #incr(byte[]) INCR} but instead to increment
	 * by 1 the increment is integer.
	 * <p>
	 * INCR commands are limited to 64 bit signed integers.
	 * <p>
	 * Note: this is actually a string operation, that is, in Redis there are
	 * not "integer" types. Simply the string stored at the key is parsed as a
	 * base 10 64 bit signed integer, incremented, and then converted back as a
	 * string.
	 * <p>
	 * 
	 * @see #incr(byte[])
	 * @see #decr(byte[])
	 * @see #decrBy(byte[], long)
	 * 
	 * @param key
	 * @param integer
	 * @return Integer reply, this commands will reply with the new value of key
	 *         after the increment.
	 */
	public Long incrBy(final byte[] key, final long integer) {

		if (simpleTypes.containsKey(wrap(key))) {
			return incrementAndSetValue(key, integer);
		} else {
			simpleTypes.put(wrap(key), ZERO);
			return incrementAndSetValue(key, integer);
		}

	}

	/**
	 * Increment the number stored at key by one. If the key does not exist or
	 * contains a value of a wrong type, set the key to the value of "0" before
	 * to perform the increment operation.
	 * <p>
	 * INCR commands are limited to 64 bit signed integers.
	 * <p>
	 * Note: this is actually a string operation, that is, in Redis there are
	 * not "integer" types. Simply the string stored at the key is parsed as a
	 * base 10 64 bit signed integer, incremented, and then converted back as a
	 * string.
	 * <p>
	 * 
	 * @see #incrBy(byte[], long)
	 * @see #decr(byte[])
	 * @see #decrBy(byte[], long)
	 * 
	 * @param key
	 * @return Integer reply, this commands will reply with the new value of key
	 *         after the increment.
	 */
	public Long incr(final byte[] key) {
		return incrBy(key, 1);
	}

	private long incrementAndSetValue(final byte[] key, final long integer) {
		byte[] oldValue = simpleTypes.get(wrap(key)).array();
		long newValue = incrementValue(oldValue, integer);
		byte[] newValueByteArray = SafeEncoder.encode(Long.toString(newValue));
		simpleTypes.put(wrap(key), wrap(newValueByteArray));

		return newValue;
	}

	private long incrementValue(byte[] value, long increment) {
		String numberEncode = SafeEncoder.encode(value);
		long longValue = Long.parseLong(numberEncode);
		longValue += increment;

		return longValue;
	}

	/**
	 * Returns the bit value at offset in the string value stored at key
	 * 
	 * @param key
	 * @param offset
	 * @return
	 */
	public Boolean getbit(byte[] key, long offset) {

		if (simpleTypes.containsKey(wrap(key))) {

			try {

				byte[] value = simpleTypes.get(wrap(key)).array();
				int bit = BitsUtils.getBit(value, (int) offset);

				return BitsUtils.toBoolean(bit);

			} catch (ArrayIndexOutOfBoundsException e) {
				return Boolean.FALSE;
			}

		} else {
			return Boolean.FALSE;
		}

	}
	
	 /**
     * Sets or clears the bit at offset in the string value stored at key
     * 
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public Boolean setbit(byte[] key, long offset, byte[] value) {
    	
    	boolean originalValue = getbit(key, offset);
    	
    	int realValue = Integer.parseInt(SafeEncoder.encode(value));
    	
    	int numberOfBytesRequired = BitsUtils.calculateNumberOfBytes((int)offset);
    	if (simpleTypes.containsKey(wrap(key))) {

				byte[] currentValue = simpleTypes.get(wrap(key)).array();
				
				if(numberOfBytesRequired > currentValue.length) {
					currentValue = BitsUtils.extendByteArrayBy(currentValue, numberOfBytesRequired-currentValue.length);
				}
				
				BitsUtils.setBit(currentValue, (int)offset, realValue);
				simpleTypes.put(wrap(key), wrap(currentValue));

				return originalValue;
				
		} else {
			byte[] values = new byte[numberOfBytesRequired];
			BitsUtils.setBit(values, (int)offset, realValue);
			simpleTypes.put(wrap(key), wrap(values));
			
			return Boolean.FALSE;
		}
    	
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
    	
    	if (simpleTypes.containsKey(wrap(key))) {

			byte[] currentValue = simpleTypes.get(wrap(key)).array();
			
			if(offset + value.length > currentValue.length) {
				currentValue = BitsUtils.extendByteArrayBy(currentValue, (int) ((offset + value.length)-currentValue.length));
			}
			
			System.arraycopy(value, 0, currentValue, (int)offset, value.length);
			
			simpleTypes.put(wrap(key), wrap(currentValue));
			
			return (long) currentValue.length;
			
		} else {
			return append(key, value);
		}
    	
    }
    
	public byte[] getrange(byte[] key, long startOffset, long endOffset) {

		if (simpleTypes.containsKey(wrap(key))) {

			byte[] value = simpleTypes.get(wrap(key)).array();
			
			int calculatedStart = RangeUtils.calculateStart((int)startOffset, value.length);
			int calculatedEnd = RangeUtils.calculateEnd((int)endOffset, value.length);

			try {
				
				return Arrays.copyOfRange(value, calculatedStart, calculatedEnd);

			} catch (ArrayIndexOutOfBoundsException e) {
				return SafeEncoder.encode("");
			} catch(IllegalArgumentException e) {
				return SafeEncoder.encode("");
			}
		} else {
			return SafeEncoder.encode("");
		}
	}

	public String mset(final byte[]... keysvalues) {

		if ((keysvalues.length % 2) != 0)
			return null;
		for (int index = 0; index < keysvalues.length; index += 2) {
			simpleTypes.put(wrap(keysvalues[index]), wrap(keysvalues[index + 1]));
		}
		return OK;
	}

	public Long msetnx(final byte[]... keysvalues) {

		for (int index = 0; index < keysvalues.length; index += 2) {
			if (simpleTypes.containsKey(wrap(keysvalues[index])))
				return NONE_SUCCESS;
		}
		for (int index = 0; index < keysvalues.length; index += 2) {
			simpleTypes.put(wrap(keysvalues[index]), wrap(keysvalues[index + 1]));
		}
		return SUCCESS;
	}

	public int strlen(byte[] key) {
		/**
		 * Returns the length of the string value stored at key. An error is
		 * returned when key holds a non-string value. Return value Integer
		 * reply: the length of the string at key, or 0 when key does not exist.
		 */
		ByteBuffer byteBufferKey = wrap(key);
		if (!simpleTypes.containsKey(byteBufferKey))
			return 0;

		return (simpleTypes.get(byteBufferKey)).array().length;
	}

	public String rename(byte[] oldKey, byte[] newKey) {
		/**
		 * Renames key to newkey. It returns an error when the source and
		 * destination names are the same, or when key does not exist. If newkey
		 * already exists it is overwritten.
		 **/
		if (Arrays.equals(oldKey, newKey) || !simpleTypes.containsKey(wrap(oldKey))) {
			return null;
		} else {
			ByteBuffer value = simpleTypes.get(oldKey);
			simpleTypes.remove(oldKey);
			simpleTypes.put(wrap(newKey), value);
			
			renameTtlKey(oldKey, newKey);
			
			return OK;
		}
	}

	public List<byte[]> mget(byte[]... keys) {

		List<byte[]> values = new ArrayList<byte[]>();
		for (byte[] key : keys) {
			if (simpleTypes.get(wrap(key)) == null) {
				values.add(null);
			} else {
				values.add((simpleTypes.get(wrap(key))).array());
			}
		}
		return values;
	}

	public byte[] get(byte[] key) {
		return (simpleTypes.get(wrap(key))) == null ? null : (simpleTypes.get(wrap(key))).array();
	}

	public byte[] getSet(byte[] key, byte[] value) {
		/**
		 * Atomically sets key to value and returns the old value stored at key.
		 * Returns an error when key exists but does not hold a string value.
		 */
		ByteBuffer byteBufferKey = wrap(key);
		if (!simpleTypes.containsKey(byteBufferKey)) {
			simpleTypes.put(wrap(key), wrap(value));
			return null;
		} else {
			ByteBuffer oldValue = simpleTypes.get(byteBufferKey);
			simpleTypes.put(wrap(key), wrap(value));
			return (oldValue).array();
		}
	}

	public String set(byte[] key, byte[] value) {
		simpleTypes.put(wrap(key), wrap(value));
		return OK;
	}

	public Long setnx(byte[] key, byte[] value) {
		ByteBuffer byteBufferKey = wrap(key);
		if (!simpleTypes.containsKey(byteBufferKey)) {
			set(key, value);
			return SUCCESS;
		}
		return NONE_SUCCESS;
	}

	public String setex(final byte[] key, final int seconds, final byte[] value) {
		String result = this.set(key, value);
		this.addExpirationTime(key, seconds, TimeUnit.SECONDS);
		return result;
	}
	
	/**
     * Return a subset of the string from offset start to offset end (both
     * offsets are inclusive). Negative offsets can be used in order to provide
     * an offset starting from the end of the string. So -1 means the last char,
     * -2 the penultimate and so forth.
     * <p>
     * The function handles out of range requests without raising an error, but
     * just limiting the resulting range to the actual length of the string.
     * <p>
     * 
     * @param key
     * @param start
     * @param end
     * @return Bulk reply
     */
    public byte[] substr(final byte[] key, final int start, final int end) {
    	return getrange(key, start, end);
    }
	
	public long getNumberOfKeys() {
		return this.simpleTypes.keySet().size();
	}
	
	public void flushAllKeys() {
		this.removeExpirations();
		this.simpleTypes.clear();
	}

	private void removeExpirations() {
		List<byte[]> keys = this.keys();
		for (byte[] key : keys) {
			this.removeExpiration(key);
		}
	}
	
	@Override
	public Long del(byte[]... keys) {

		long numberOfRemovedElements = 0;
		
		for (byte[] key : keys) {
			ByteBuffer wrappedKey = wrap(key);
			if(this.simpleTypes.containsKey(wrappedKey)) {
				this.simpleTypes.remove(wrappedKey);
				removeExpiration(key);
				numberOfRemovedElements++;
			}
		}
		
		return numberOfRemovedElements;
	}

	
	
	@Override
	public boolean exists(byte[] key) {
		return this.simpleTypes.containsKey(wrap(key));
	}

	@Override
	public boolean renameKey(byte[] key, byte[] newKey) {
		ByteBuffer wrappedKey = wrap(key);

		if (this.simpleTypes.containsKey(wrappedKey)) {
			ByteBuffer element = this.simpleTypes.get(wrappedKey);
			this.simpleTypes.remove(wrap(newKey));
			this.simpleTypes.put(wrap(newKey), element);
			this.simpleTypes.remove(wrappedKey);
			
			renameTtlKey(key, newKey);
			
			return true;
		}

		return false;
	}

	@Override
	public List<byte[]> keys() {
		return new ArrayList<byte[]>(convert(this.simpleTypes.keySet(),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	@Override
	public String type() {
		return STRING;
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		throw new UnsupportedOperationException();
	}

	
}
