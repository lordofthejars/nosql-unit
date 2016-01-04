package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.convert;
import static com.lordofthejars.nosqlunit.redis.embedded.ByteArrayIncrement.incrementValue;
import static java.nio.ByteBuffer.wrap;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.util.JedisByteHashMap;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class HashDatatypeOperations extends ExpirationDatatypeOperations implements RedisDatatypeOperations {

	protected static final String HASH = "hash";
	protected Table<ByteBuffer, ByteBuffer, ByteBuffer> hashElements = HashBasedTable.create();

	/**
	 * 
	 * Set the specified hash field to the specified value.
	 * <p>
	 * If key does not exist, a new key holding a hash is created.
	 * <p>
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @return If the field already exists, and the HSET just produced an update
	 *         of the value, 0 is returned, otherwise if a new field is created
	 *         1 is returned.
	 */
	public Long hset(final byte[] key, final byte[] field, final byte[] value) {
		long result = 0L;
		
		if(hashElements.put(wrap(key), wrap(field), wrap(value)) == null) {
			result = 1L;			
		} 
		
		return result;
	}

	/**
	 * If key holds a hash, retrieve the value associated to the specified
	 * field.
	 * <p>
	 * If the field is not found or the key does not exist, a special 'nil'
	 * value is returned.
	 * <p>
	 * 
	 * @param key
	 * @param field
	 * @return Bulk reply
	 */
	public byte[] hget(final byte[] key, final byte[] field) {
		ByteBuffer result = hashElements.get(wrap(key), wrap(field));
		return arrayValueOrNull(result);
	}

	/**
	 * Remove the specified field from an hash stored at key.
	 * <p>
	 * <b>Time complexity:</b> O(1)
	 * 
	 * @param key
	 * @param fields
	 * @return If the field was present in the hash it is deleted and 1 is
	 *         returned, otherwise 0 is returned and no operation is performed.
	 */
	public Long hdel(final byte[] key, final byte[]... fields) {

		long numberOfRemovedFields = 0L;

		for (byte[] field : fields) {
			if (hashElements.remove(wrap(key), wrap(field)) != null) {
				numberOfRemovedFields++;
			}
		}

		return numberOfRemovedFields;

	}

	/**
	 * Test for existence of a specified field in a hash.
	 * 
	 * <b>Time complexity:</b> O(1)
	 * 
	 * @param key
	 * @param field
	 * @return Return 1 if the hash stored at key contains the specified field.
	 *         Return 0 if the key is not found or the field is not present.
	 */
	public Boolean hexists(final byte[] key, final byte[] field) {
		return hashElements.contains(wrap(key), wrap(field));
	}

	/**
	 * Return all the fields and associated values in a hash.
	 * <p>
	 * 
	 * @param key
	 * @return All the fields and values contained into a hash.
	 */
	public Map<byte[], byte[]> hgetAll(final byte[] key) {

		Map<ByteBuffer, ByteBuffer> row = hashElements.row(wrap(key));
		return transformMapToByteArray(row);

	}

	 /**
     * Increment the number stored at field in the hash at key by value. If key
     * does not exist, a new key holding a hash is created. If field does not
     * exist or holds a string, the value is set to 0 before applying the
     * operation. Since the value argument is signed you can use this command to
     * perform both increments and decrements.
     * <p>
     * The range of values supported by HINCRBY is limited to 64 bit signed
     * integers.
     * <p>
     * 
     * @param key
     * @param field
     * @param value
     * @return Integer reply The new value at field after the increment
     *         operation.
     */
	public Long hincrBy(final byte[] key, final byte[] field, final long value) {
		byte[] elementToUpdate = hget(key, field);

		if (elementToUpdate != null) {
			return incrementAndSetValue(key, field, value, elementToUpdate);
		} else {
			return setLongValue(key, field, value);
		}
	}

	/**
	 * Return all the fields in a hash.
	 * <p>
	 * 
	 * @param key
	 * @return All the fields names contained into a hash.
	 */
	public Set<byte[]> hkeys(final byte[] key) {

		Set<ByteBuffer> columnKeySet = getAllFieldsNameByKey(key);
		return new HashSet<byte[]>(convert(columnKeySet,
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	/**
	 * Return the number of items in a hash.
	 * <p>
	 * 
	 * @param key
	 * @return The number of entries (fields) contained in the hash stored at
	 *         key. If the specified key does not exist, 0 is returned assuming
	 *         an empty hash.
	 */
	public Long hlen(final byte[] key) {
		return (long) getAllFieldsNameByKey(key).size();
	}
	
	/**
	 * Retrieve the values associated to the specified fields.
	 * <p>
	 * If some of the specified fields do not exist, nil values are returned.
	 * Non existing keys are considered like empty hashes.
	 * <p>
	 * 
	 * @param key
	 * @param fields
	 * @return Multi Bulk Reply specifically a list of all the values associated
	 *         with the specified fields, in the same order of the request.
	 */
	public List<byte[]> hmget(final byte[] key, final byte[]... fields) {

		List<byte[]> fieldsValues = new ArrayList<byte[]>();

		for (byte[] field : fields) {
			fieldsValues.add(hget(key, field));
		}

		return fieldsValues;

	}

	/**
	 * Set the respective fields to the respective values. HMSET replaces old
	 * values with new values.
	 * <p>
	 * If key does not exist, a new key holding a hash is created.
	 * <p>
	 * 
	 * @param key
	 * @param hash
	 * @return Always OK because HMSET can't fail
	 */
	public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {

		Set<Entry<byte[], byte[]>> fields = hash.entrySet();

		for (Entry<byte[], byte[]> entry : fields) {
			hset(key, entry.getKey(), entry.getValue());
		}

		return "OK";

	}

	/**
	 * 
	 * Set the specified hash field to the specified value if the field not
	 * exists. <b>Time complexity:</b> O(1)
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @return If the field already exists, 0 is returned, otherwise if a new
	 *         field is created 1 is returned.
	 */
	public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
		if (hexists(key, field)) {
			return 0L;
		} else {
			return hset(key, field, value);
		}
	}
	
	/**
	 * Return all the values in a hash.
	 * <p>
	 * 
	 * @param key
	 * @return All the fields values contained into a hash.
	 */
	public List<byte[]> hvals(final byte[] key) {
		return new ArrayList<byte[]>(convert(getAllFieldsValueByKey(key),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	public long getNumberOfKeys() {
		return this.hashElements.rowKeySet().size();
	}
	
	public void flushAllKeys() {
		removeExpirations();
		this.hashElements.clear();
	}

	private void removeExpirations() {
		List<byte[]> keys = this.keys();
		for (byte[] key : keys) {
			this.removeExpiration(key);
		}
	}
	
	private Long setLongValue(final byte[] key, final byte[] field, final long value) {
		try {
			hset(key, field, Long.toString(value).getBytes("UTF-8"));
			return value;
		} catch (UnsupportedEncodingException e) {
			throw new UnsupportedOperationException("ERR hash value is not an integer");
		}
	}

	private Long incrementAndSetValue(final byte[] key, final byte[] field, final long value, byte[] elementToUpdate) {
		try {
			long longValue = incrementValue(value, elementToUpdate);
			hset(key, field, Long.toString(longValue).getBytes("UTF-8"));
			return longValue;
		} catch (UnsupportedEncodingException e) {
			throw new UnsupportedOperationException("ERR value is not an integer");
		} catch(NumberFormatException e) {
			throw new UnsupportedOperationException("ERR value is not an integer"); 
		}
	}

	private Map<byte[], byte[]> transformMapToByteArray(Map<ByteBuffer, ByteBuffer> row) {
		Set<Entry<ByteBuffer, ByteBuffer>> entrySet = row.entrySet();

		final Map<byte[], byte[]> hash = new JedisByteHashMap();
		for (Entry<ByteBuffer, ByteBuffer> entry : entrySet) {
			hash.put(entry.getKey().array(), arrayValueOrNull(entry.getValue()));
		}

		return hash;
	}

	private byte[] arrayValueOrNull(ByteBuffer byteBuffer) {
		return byteBuffer == null ? null : byteBuffer.array();
	}

	private Collection<ByteBuffer> getAllFieldsValueByKey(final byte[] key) {
		Map<ByteBuffer, ByteBuffer> row = hashElements.row(wrap(key));
		Collection<ByteBuffer> columnValueCollection = row.values();
		return columnValueCollection;
	}

	private Set<ByteBuffer> getAllFieldsNameByKey(final byte[] key) {
		Map<ByteBuffer, ByteBuffer> row = hashElements.row(wrap(key));
		Set<ByteBuffer> columnKeySet = row.keySet();
		return columnKeySet;
	}
	
	@Override
	public String type() {
		return HASH;
	}

	@Override
	public List<byte[]> keys() {
		return new ArrayList<byte[]>(convert(this.hashElements.rowKeySet(),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	@Override
	public Long del(byte[]... keys) {
		long numberOfRemovedElements = 0;
		
		for (byte[] key : keys) {
			
			ByteBuffer wrappedKey = wrap(key);
			
			if(this.hashElements.containsRow(wrappedKey)) {
				deleteAllFields(wrappedKey);
				removeExpiration(key);
				numberOfRemovedElements++;
			}
		}
		
		return numberOfRemovedElements;
	}

	private void deleteAllFields(ByteBuffer key) {
		Map<ByteBuffer, ByteBuffer> fields = this.hashElements.row(key);
		Set<ByteBuffer> columns = fields.keySet();
		
		Iterator<ByteBuffer> iterator = columns.iterator();
		
		while(iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	@Override
	public boolean exists(byte[] key) {
		ByteBuffer rowKey = wrap(key);
		return this.hashElements.containsRow(rowKey);
	}

	@Override
	public boolean renameKey(byte[] key, byte[] newKey) {
		ByteBuffer wrappedKey = wrap(key);
		if(this.hashElements.containsRow(wrappedKey )) {
			Map<ByteBuffer, ByteBuffer> row = this.hashElements.row(wrappedKey);

			deleteAllFields(wrap(newKey));
			
			Set<Entry<ByteBuffer, ByteBuffer>> entries = row.entrySet();
			
			for (Entry<ByteBuffer, ByteBuffer> entry : entries) {
				hashElements.put(wrap(newKey), entry.getKey(), entry.getValue());
			}
			
			deleteAllFields(wrappedKey);
			renameTtlKey(key, newKey);
			
			return true;
		}
		return false;
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		throw new UnsupportedOperationException();
	}

	
}
