package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.collection.LambdaCollections.with;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import ch.lambdaj.function.convert.Converter;

import com.lordofthejars.nosqlunit.redis.embedded.ListDatatypeOperations.ListPositionEnum;
import com.lordofthejars.nosqlunit.redis.embedded.SortsetDatatypeOperations.ScoredByteBuffer;

public class EmbeddedJedis implements JedisCommands, BinaryJedisCommands {

	private static final ByteArrayToStringConverter BYTE_ARRAY_TO_STRING_CONVERTER = ByteArrayToStringConverter
			.createByteArrayToStringConverter();
	private static final StringToByteArrayConverter STRING_TO_BYTE_ARRAY_CONVERTER = StringToByteArrayConverter
			.createStringToByteArrayConverter();

	protected HashDatatypeOperations hashDatatypeOperations;
	protected ListDatatypeOperations listDatatypeOperations;
	protected SetDatatypeOperations setDatatypeOperations;
	protected SortsetDatatypeOperations sortsetDatatypeOperations;
	protected StringDatatypeOperations stringDatatypeOperations;
	protected KeysServerOperations keysServerOperations;

	public EmbeddedJedis() {
		hashDatatypeOperations = new HashDatatypeOperations();
		listDatatypeOperations = new ListDatatypeOperations();
		setDatatypeOperations = new SetDatatypeOperations();
		sortsetDatatypeOperations = new SortsetDatatypeOperations();
		stringDatatypeOperations = new StringDatatypeOperations();
		keysServerOperations = KeysServerOperations.createKeysServerOperations(hashDatatypeOperations,
				listDatatypeOperations, setDatatypeOperations, sortsetDatatypeOperations, stringDatatypeOperations);
	}

	@Override
	public String set(byte[] key, byte[] value) {
		keysServerOperations.del(key);
		stringDatatypeOperations.removeExpiration(key);
		return stringDatatypeOperations.set(key, value);
	}

	@Override
	public byte[] get(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);

		return stringDatatypeOperations.get(key);
	}

	@Override
	public Boolean exists(byte[] key) {
		updateTtl(key);
		return this.stringDatatypeOperations.exists(key);
	}

	@Override
	public String type(byte[] key) {
		updateTtl(key);
		return this.keysServerOperations.type(key);
	}

	@Override
	public Long expire(byte[] key, int seconds) {
		return this.keysServerOperations.expire(key, seconds);
	}

	@Override
	public Long expireAt(byte[] key, long unixTime) {
		return this.keysServerOperations.expireAt(key, unixTime);
	}

	@Override
	public Long ttl(byte[] key) {
		return this.keysServerOperations.ttl(key);
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		stringDatatypeOperations.removeExpiration(key);

		return this.stringDatatypeOperations.getSet(key, value);
	}

	@Override
	public Long setnx(byte[] key, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.setnx(key, value);
	}

	@Override
	public String setex(byte[] key, int seconds, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.setex(key, seconds, value);
	}

	@Override
	public Long decrBy(byte[] key, long integer) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.decrBy(key, integer);
	}

	@Override
	public Long decr(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.decr(key);
	}

	@Override
	public Long incrBy(byte[] key, long integer) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.incrBy(key, integer);
	}

	@Override
	public Long incr(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.incr(key);
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.append(key, value);
	}

	@Override
	public byte[] substr(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.substr(key, start, end);
	}

	@Override
	public Long hset(byte[] key, byte[] field, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hset(key, field, value);
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hget(key, field);
	}

	@Override
	public Long hsetnx(byte[] key, byte[] field, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hsetnx(key, field, value);
	}

	@Override
	public String hmset(byte[] key, Map<byte[], byte[]> hash) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hmset(key, hash);
	}

	@Override
	public List<byte[]> hmget(byte[] key, byte[]... fields) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hmget(key, fields);
	}

	@Override
	public Long hincrBy(byte[] key, byte[] field, long value) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hincrBy(key, field, value);
	}

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hexists(key, field);
	}

	@Override
	public Long hdel(byte[] key, byte[]... fields) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hdel(key, fields);
	}

	@Override
	public Long hlen(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hlen(key);
	}

	@Override
	public Set<byte[]> hkeys(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hkeys(key);
	}

	@Override
	public Collection<byte[]> hvals(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hvals(key);
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
		return this.hashDatatypeOperations.hgetAll(key);
	}

	@Override
	public Long rpush(byte[] key, byte[]... values) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.rpush(key, values);
	}

	@Override
	public Long lpush(byte[] key, byte[]... values) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lpush(key, values);
	}

	@Override
	public Long llen(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.llen(key);
	}

	@Override
	public List<byte[]> lrange(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lrange(key, start, end);
	}

	@Override
	public String ltrim(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.ltrim(key, start, end);
	}

	@Override
	public byte[] lindex(byte[] key, int index) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lindex(key, index);
	}

	@Override
	public String lset(byte[] key, int index, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lset(key, index, value);
	}

	@Override
	public Long lrem(byte[] key, int count, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lrem(key, count, value);
	}

	@Override
	public byte[] lpop(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lpop(key);
	}

	@Override
	public byte[] rpop(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.rpop(key);
	}

	@Override
	public Long sadd(byte[] key, byte[]... member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.sadd(key, member);
	}

	@Override
	public Set<byte[]> smembers(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.smembers(key);
	}

	@Override
	public Long srem(byte[] key, byte[]... member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.srem(key, member);
	}

	@Override
	public byte[] spop(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.spop(key);
	}

	@Override
	public Long scard(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.scard(key);
	}

	@Override
	public Boolean sismember(byte[] key, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.sismember(key, member);
	}

	@Override
	public byte[] srandmember(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, SetDatatypeOperations.SET);
		return this.setDatatypeOperations.srandmember(key);
	}

	@Override
	public Long zadd(byte[] key, double score, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zadd(key, score, member);
	}

	@Override
	public Long zadd(byte[] key, Map<Double, byte[]> scoreMembers) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zadd(key, scoreMembers);
	}

	@Override
	public Set<byte[]> zrange(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrange(key, start, end);
	}

	@Override
	public Long zrem(byte[] key, byte[]... members) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrem(key, members);
	}

	@Override
	public Double zincrby(byte[] key, double score, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zincrby(key, score, member);
	}

	@Override
	public Long zrank(byte[] key, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrank(key, member);
	}

	@Override
	public Long zrevrank(byte[] key, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrank(key, member);
	}

	@Override
	public Set<byte[]> zrevrange(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrange(key, start, end);
	}

	@Override
	public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> rangeWithScore = this.sortsetDatatypeOperations.zrangeWithScores(key, start, end);
		return new LinkedHashSet<Tuple>(with(rangeWithScore).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> rangeWithScore = this.sortsetDatatypeOperations.zrevrangeWithScores(key, start, end);
		return new LinkedHashSet<Tuple>(with(rangeWithScore).convert(toTuple()));
	}

	@Override
	public Long zcard(byte[] key) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zcard(key);
	}

	@Override
	public Double zscore(byte[] key, byte[] member) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zscore(key, member);
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		updateTtl(key);
		return this.keysServerOperations.sort(key);
	}

	@Override
	public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
		throw new UnsupportedOperationException("Sort with parameters is not supported.");
	}

	@Override
	public Long zcount(byte[] key, double min, double max) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zcount(key, min, max);
	}

	@Override
	public Long zcount(byte[] key, byte[] min, byte[] max) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zcount(key, min, max);
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrangeByScore(key, min, max);
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
				min, max);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
				min, max, offset, count);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
				min, max);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
				min, max, offset, count);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
				max, min);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
				max, min, offset, count);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
				max, min);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
				max, min, offset, count);
		return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
	}

	@Override
	public Long zremrangeByRank(byte[] key, int start, int end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zremrangeByRank(key, start, end);
	}

	@Override
	public Long zremrangeByScore(byte[] key, double start, double end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zremrangeByScore(key, start, end);
	}

	@Override
	public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
		updateTtl(key);
		checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
		return this.sortsetDatatypeOperations.zremrangeByScore(key, start, end);
	}

	@Override
	public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.linsert(key, toListPosition().convert(where), pivot, value);
	}

	@Override
	public Long objectRefcount(byte[] key) {
		throw new UnsupportedOperationException("Object ref count is not supported.");
	}

	@Override
	public Long objectIdletime(byte[] key) {
		throw new UnsupportedOperationException("Object idle time is not supported.");
	}

	@Override
	public byte[] objectEncoding(byte[] key) {
		throw new UnsupportedOperationException("Object encoding is not supported.");
	}

	@Override
	public Long lpushx(byte[] key, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.lpushx(key, value);
	}

	@Override
	public Long rpushx(byte[] key, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
		return this.listDatatypeOperations.rpushx(key, value);
	}

	public Boolean setbit(byte[] key, long offset, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.setbit(key, offset, value);
	}

	public Boolean getbit(byte[] key, long offset) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.getbit(key, offset);
	}

	public Long setrange(byte[] key, long offset, byte[] value) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.setrange(key, offset, value);
	}
	
	public byte[] getrange(byte[] key, long startOffset, long endOffset) {
		updateTtl(key);
		checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
		return this.stringDatatypeOperations.getrange(key, startOffset, endOffset);
	}
	
	@Override
	public String set(String key, String value) {
		return this.set(toByteArray().convert(key), toByteArray().convert(value));
	}

	@Override
	public String get(String key) {
		byte[] result = this.get(toByteArray().convert(key));
		return toStringValue().convert(result);
	}

	@Override
	public Boolean exists(String key) {
		return this.exists(toByteArray().convert(key));
	}

	@Override
	public String type(String key) {
		return this.type(toByteArray().convert(key));
	}

	@Override
	public Long expire(String key, int seconds) {
		return this.expire(toByteArray().convert(key), seconds);
	}

	@Override
	public Long expireAt(String key, long unixTime) {
		return expireAt(toByteArray().convert(key), unixTime);
	}

	@Override
	public Long ttl(String key) {
		return this.ttl(toByteArray().convert(key));
	}

	@Override
	public Boolean setbit(String key, long offset, boolean value) {
		return this.setbit(toByteArray().convert(key), offset, toBooleanByteArray().convert(value));
	}

	@Override
	public Boolean getbit(String key, long offset) {
		return this.getbit(toByteArray().convert(key), offset);
	}

	@Override
	public Long setrange(String key, long offset, String value) {
		return this.setrange(toByteArray().convert(key), offset, toByteArray().convert(value));
	}

	@Override
	public String getrange(String key, long startOffset, long endOffset) {
		byte[] result = this.getrange(toByteArray().convert(key), startOffset, endOffset);
		return toStringValue().convert(result);
	}

	@Override
	public String getSet(String key, String value) {
		byte[] result = this.getSet(toByteArray().convert(key), toByteArray().convert(value));
		return toStringValue().convert(result);
	}

	@Override
	public Long setnx(String key, String value) {
		return this.setnx(toByteArray().convert(key), toByteArray().convert(value));
	}

	@Override
	public String setex(String key, int seconds, String value) {
		return this.setex(toByteArray().convert(key), seconds, toByteArray().convert(value));
	}

	@Override
	public Long decrBy(String key, long integer) {
		return this.decrBy(toByteArray().convert(key), integer);
	}

	@Override
	public Long decr(String key) {
		return this.decr(toByteArray().convert(key));
	}

	@Override
	public Long incrBy(String key, long integer) {
		return this.incrBy(toByteArray().convert(key), integer);
	}

	@Override
	public Long incr(String key) {
		return this.incr(toByteArray().convert(key));
	}

	@Override
	public Long append(String key, String value) {
		return this.append(toByteArray().convert(key), toByteArray().convert(value));
	}

	@Override
	public String substr(String key, int start, int end) {
		byte[] result = this.substr(toByteArray().convert(key), start, end);
		return toStringValue().convert(result);
	}

	@Override
	public Long hset(String key, String field, String value) {
		return this.hset(toByteArray().convert(key), toByteArray().convert(field), toByteArray().convert(value));
	}

	@Override
	public String hget(String key, String field) {
		byte[] result = this.hget(toByteArray().convert(key), toByteArray().convert(field));
		return toStringValue().convert(result);
	}

	@Override
	public Long hsetnx(String key, String field, String value) {
		return this.hsetnx(toByteArray().convert(key), toByteArray().convert(field), toByteArray().convert(value));
	}

	@Override
	public String hmset(String key, Map<String, String> hash) {
		return this.hmset(toByteArray().convert(key), toMapByteArray().convert(hash));
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
		List<byte[]> result = this.hmget(toByteArray().convert(key), arrayOfFields);
		return with(result).convert(toStringValue());
	}

	@Override
	public Long hincrBy(String key, String field, long value) {
		return this.hincrBy(toByteArray().convert(key), toByteArray().convert(field), value);
	}

	@Override
	public Boolean hexists(String key, String field) {
		return hexists(toByteArray().convert(key), toByteArray().convert(field));
	}

	@Override
	public Long hdel(String key, String... fields) {
		byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
		return this.hdel(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public Long hlen(String key) {
		return this.hlen(toByteArray().convert(key));
	}

	@Override
	public Set<String> hkeys(String key) {
		Set<byte[]> result = this.hkeys(toByteArray().convert(key));
		return new LinkedHashSet<String>(with(result).convert(toStringValue()));
	}

	@Override
	public List<String> hvals(String key) {
		Collection<byte[]> result = this.hvals(toByteArray().convert(key));
		return new LinkedList<String>(with(result).convert(toStringValue()));
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		Map<byte[], byte[]> result = this.hgetAll(toByteArray().convert(key));
		return toMapString().convert(result);
	}

	@Override
	public Long rpush(String key, String... fields) {
		byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
		return this.rpush(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public Long lpush(String key, String... fields) {
		byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
		return this.lpush(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public Long llen(String key) {
		return this.llen(toByteArray().convert(key));
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		List<byte[]> result = this.lrange(toByteArray().convert(key), (int)start, (int)end);
		return with(result).convert(toStringValue());
	}

	@Override
	public String ltrim(String key, long start, long end) {
		return this.ltrim(toByteArray().convert(key), (int)start, (int)end);
	}

	@Override
	public String lindex(String key, long index) {
		byte[] result = this.lindex(toByteArray().convert(key), (int)index);
		return toStringValue().convert(result);
	}

	@Override
	public String lset(String key, long index, String value) {
		return this.lset(toByteArray().convert(key), (int)index, toByteArray().convert(value));
	}

	@Override
	public Long lrem(String key, long count, String value) {
		return this.lrem(toByteArray().convert(key), (int)count, toByteArray().convert(value));
	}

	@Override
	public String lpop(String key) {
		byte[] result = this.lpop(toByteArray().convert(key));
		return toStringValue().convert(result);
	}

	@Override
	public String rpop(String key) {
		byte[] result = this.rpop(toByteArray().convert(key));
		return toStringValue().convert(result);
	}

	@Override
	public Long sadd(String key, String... members) {
		byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
		return this.sadd(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public Set<String> smembers(String key) {
		Set<byte[]> result = this.smembers(toByteArray().convert(key));
		return new LinkedHashSet<String>(with(result).convert(toStringValue()));
	}

	@Override
	public Long srem(String key, String... members) {
		byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
		return this.srem(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public String spop(String key) {
		byte[] result = this.spop(toByteArray().convert(key));
		return toStringValue().convert(result);
	}

	@Override
	public Long scard(String key) {
		return this.scard(toByteArray().convert(key));
	}

	@Override
	public Boolean sismember(String key, String member) {
		return this.sismember(toByteArray().convert(key), toByteArray().convert(member));
	}

	@Override
	public String srandmember(String key) {
		byte[] result = this.srandmember(toByteArray().convert(key));
		return toStringValue().convert(result);
	}

	@Override
	public Long zadd(String key, double score, String member) {
		return this.zadd(toByteArray().convert(key), score, toByteArray().convert(member));
	}

	@Override
	public Long zadd(String key, Map<Double, String> scoreMembers) {
		return this.zadd(toByteArray().convert(key), with(scoreMembers).convertValues(toByteArray()));
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		Set<byte[]> result = this.zrange(toByteArray().convert(key), (int)start, (int)end);
		return new HashSet<String>(with(result).convert(toStringValue()));
	}

	@Override
	public Long zrem(String key, String... members) {
		byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
		return this.zrem(toByteArray().convert(key), arrayOfFields);
	}

	@Override
	public Double zincrby(String key, double score, String member) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zrank(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zrevrank(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrevrange(String key, long start, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zcard(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double zscore(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> sort(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> sort(String key, SortingParams sortingParameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zcount(String key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zcount(String key, String min, String max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zremrangeByRank(String key, long start, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zremrangeByScore(String key, double start, double end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zremrangeByScore(String key, String start, String end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long lpushx(String key, String string) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long rpushx(String key, String string) {
		// TODO Auto-generated method stub
		return null;
	}

	private void checkValidTypeOrNone(byte[] key, String type) {
		String currentType = keysServerOperations.type(key);
		if (!(KeysServerOperations.NONE.equals(currentType) || currentType.equals(type))) {
			throw new IllegalArgumentException("ERR Operation against a key holding the wrong kind of value");
		}
	}

	private void updateTtl(byte[] key) {
		this.keysServerOperations.updateTtl(key);
	}

	private Converter<ScoredByteBuffer, Tuple> toTuple() {
		return new Converter<SortsetDatatypeOperations.ScoredByteBuffer, Tuple>() {

			@Override
			public Tuple convert(ScoredByteBuffer from) {
				return new Tuple(from.getByteBuffer().array(), from.getScore());
			}
		};
	}

	private Converter<LIST_POSITION, ListPositionEnum> toListPosition() {
		return new Converter<BinaryClient.LIST_POSITION, ListDatatypeOperations.ListPositionEnum>() {

			@Override
			public ListPositionEnum convert(LIST_POSITION from) {
				switch (from) {
				case BEFORE:
					return ListPositionEnum.BEFORE;
				case AFTER:
					return ListPositionEnum.AFTER;
				default:
					return ListPositionEnum.BEFORE;
				}
			}
		};
	}

	private Converter<Boolean, byte[]> toBooleanByteArray() {
		return new Converter<Boolean, byte[]>() {

			@Override
			public byte[] convert(Boolean from) {
				if (from) {
					return "1".getBytes();
				} else {
					return "0".getBytes();
				}
			}
		};
	}

	private Converter<Map<byte[], byte[]>, Map<String, String>> toMapString() {
		return new Converter<Map<byte[],byte[]>, Map<String, String>>() {
			
			@Override
			public Map<String, String> convert(Map<byte[], byte[]> from) {
				
				Map<String, String> map = new LinkedHashMap<String, String>(from.size());
				
				Set<byte[]> keySet = from.keySet();
				
				for (byte[] key : keySet) {
					map.put(toStringValue().convert(key), toStringValue().convert(from.get(key)));
				}
				
				return map;
			}
		};
	}
	
	private Converter<Map<String, String>, Map<byte[], byte[]>> toMapByteArray() {
		return new Converter<Map<String,String>, Map<byte[],byte[]>>() {
			
			@Override
			public Map<byte[], byte[]> convert(Map<String, String> from) {
				
				Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>(from.size());
				
				Set<String> keySet = from.keySet();
				
				for (String key : keySet) {
					map.put(toByteArray().convert(key), toByteArray().convert(from.get(key)));
				}
				
				return map;
			}
		};
	}
	
	private Converter<byte[], String> toStringValue() {
		return BYTE_ARRAY_TO_STRING_CONVERTER;
	}

	private Converter<String, byte[]> toByteArray() {
		return STRING_TO_BYTE_ARRAY_CONVERTER;
	}

}
