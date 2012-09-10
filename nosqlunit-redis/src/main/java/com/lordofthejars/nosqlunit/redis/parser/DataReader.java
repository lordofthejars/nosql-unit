package com.lordofthejars.nosqlunit.redis.parser;

import static com.lordofthejars.nosqlunit.redis.parser.JsonToJedisConverter.toByteArray;
import static com.lordofthejars.nosqlunit.redis.parser.JsonToJedisConverter.toDouble;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.BinaryJedisCommands;

public class DataReader {

	public static final String SCORE_TOKEN = "score";
	public static final String SORTSET_TOKEN = "sortset";
	public static final String VALUES_TOKEN = "values";
	public static final String LIST_TOKEN = "list";
	public static final String VALUE_TOKEN = "value";
	public static final String KEY_TOKEN = "key";
	public static final String SIMPLE_TOKEN = "simple";
	public static final String DATA_TOKEN = "data";
	public static final String HASH_TOKEN = "hash";
	public static final String FIELD_TOKEN = "field";
	public static final String EXPIRE_SEC_TOKEN = "expireSeconds";
	public static final String EXPIRE_AT_SEC_TOKEN = "expireAtSeconds";
	public static final String SET_TOKEN = "set";

	private BinaryJedisCommands jedis;

	public DataReader(BinaryJedisCommands jedis) {
		this.jedis = jedis;
	}

	public void read(InputStream data) {

		Object parse = JSONValue.parse(new InputStreamReader(data));
		JSONObject rootObject = (JSONObject) parse;

		JSONArray dataObject = (JSONArray) rootObject.get(DATA_TOKEN);

		for (Object object : dataObject) {
			JSONObject elementObject = (JSONObject) object;

			if (elementObject.containsKey(SIMPLE_TOKEN)) {
				addSimpleValues(elementObject);
			} else {
				if (elementObject.containsKey(LIST_TOKEN)) {
					addListsElement(elementObject);
				} else {
					if (elementObject.containsKey(SORTSET_TOKEN)) {
						addSortSetsElement(elementObject);
					} else {
						if (elementObject.containsKey(HASH_TOKEN)) {
							addHashesElement(elementObject);
						} else {
							if(elementObject.containsKey(SET_TOKEN)) {
								addSetsElement(elementObject);
							}
						}
					}
				}
			}
		}
	}


	private void addHashesElement(JSONObject hashesObject) {
		JSONArray sortsetsObject = (JSONArray) hashesObject.get(HASH_TOKEN);

		for (Object object : sortsetsObject) {
			JSONObject hashObject = (JSONObject) object;
			addHashElements(hashObject);
		}

	}

	private void addHashElements(JSONObject hashesObject) {

		Object key = hashesObject.get(KEY_TOKEN);
		JSONArray valuesArray = (JSONArray) hashesObject.get(VALUES_TOKEN);

		Map<byte[], byte[]> fields = new HashMap<byte[], byte[]>();

		for (Object object : valuesArray) {
			JSONObject fieldObject = (JSONObject) object;
			fields.put(toByteArray(fieldObject.get(FIELD_TOKEN)), toByteArray(fieldObject.get(VALUE_TOKEN)));
		}

		this.jedis.hmset(toByteArray(key), fields);
		setTTL(hashesObject, key);
	}

	private void addSortSetsElement(JSONObject elementObject) {

		JSONArray sortsetsObject = (JSONArray) elementObject.get(SORTSET_TOKEN);

		for (Object object : sortsetsObject) {
			JSONObject sortsetObject = (JSONObject) object;
			addSortSetElements(sortsetObject);
		}

	}

	private void addSortSetElements(JSONObject sortsetObject) {
		Object key = sortsetObject.get(KEY_TOKEN);
		JSONArray valuesArray = (JSONArray) sortsetObject.get(VALUES_TOKEN);

		Map<Double, byte[]> scoreMembers = new HashMap<Double, byte[]>();

		for (Object valueObject : valuesArray) {
			JSONObject valueScopeObject = (JSONObject) valueObject;
			scoreMembers.put(toDouble(valueScopeObject.get(SCORE_TOKEN)),
					toByteArray(valueScopeObject.get(VALUE_TOKEN)));
		}

		this.jedis.zadd(toByteArray(key), scoreMembers);
		setTTL(sortsetObject, key);
	}

	private void addSetsElement(JSONObject elementObject) {
		JSONArray setObjects = (JSONArray) elementObject.get(SET_TOKEN);

		for (Object object : setObjects) {
			JSONObject setObject = (JSONObject) object;
			addSetElements(setObject);
		}
	}
	
	private void addSetElements(JSONObject setObject) {
		JSONArray valuesArray = (JSONArray) setObject.get(VALUES_TOKEN);
		List<byte[]> listValues = extractListOfValues(valuesArray);

		Object key = setObject.get(KEY_TOKEN);

		this.jedis.sadd(toByteArray(key), listValues.toArray(new byte[listValues.size()][]));
		setTTL(setObject, key);
	}

	private void addListsElement(JSONObject elementObject) {
		JSONArray listObjects = (JSONArray) elementObject.get(LIST_TOKEN);

		for (Object object : listObjects) {
			JSONObject listObject = (JSONObject) object;
			addListElements(listObject);
		}
	}

	private void addListElements(JSONObject listObject) {
		JSONArray valuesArray = (JSONArray) listObject.get(VALUES_TOKEN);
		List<byte[]> listValues = extractListOfValues(valuesArray);

		Object key = listObject.get(KEY_TOKEN);

		this.jedis.rpush(toByteArray(key), listValues.toArray(new byte[listValues.size()][]));
		setTTL(listObject, key);
	}

	private List<byte[]> extractListOfValues(JSONArray valuesArray) {
		List<byte[]> listValues = new ArrayList<byte[]>();

		for (Object valueObject : valuesArray) {
			JSONObject jsonValueObject = (JSONObject) valueObject;
			listValues.add(toByteArray(jsonValueObject.get(VALUE_TOKEN)));
		}
		return listValues;
	}

	private void addSimpleValues(JSONObject elementObject) {
		JSONArray simpleElements = (JSONArray) elementObject.get(SIMPLE_TOKEN);
		for (Object simpleElement : simpleElements) {
			JSONObject simpleElementObject = (JSONObject) simpleElement;
			Object key = simpleElementObject.get(KEY_TOKEN);
			
			this.jedis.set(toByteArray(key),
					toByteArray(simpleElementObject.get(VALUE_TOKEN)));
			setTTL(simpleElementObject, key);
		}
	}

	private void setTTL(JSONObject object, Object key) {
		
		if(object.containsKey(EXPIRE_AT_SEC_TOKEN)) {
			Object expirationDate = object.get(EXPIRE_AT_SEC_TOKEN);
			
			if(expirationDate instanceof Long) {
				this.jedis.expireAt(toByteArray(key), (Long)expirationDate);
			} else {
				throw new IllegalArgumentException("TTL expiration date should be a long value.");
			}
		}
		
		if(object.containsKey(EXPIRE_SEC_TOKEN)) {
			Object expiration = object.get(EXPIRE_SEC_TOKEN);
			
			if(expiration instanceof Long) {
				this.jedis.expire(toByteArray(key), ((Long)expiration).intValue());
			} else {
				throw new IllegalArgumentException("TTL expiration date should be an integer value.");
			}
			
		} 
		
	}
	
}
