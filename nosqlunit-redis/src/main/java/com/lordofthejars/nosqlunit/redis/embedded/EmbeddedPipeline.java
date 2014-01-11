package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisDataException;

public class EmbeddedPipeline extends Pipeline {

    private EmbeddedJedis jedis;
    private MultiResponseBuilder currentMulti;

    public EmbeddedPipeline(EmbeddedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public Response<Long> append(String key, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.append(key, value));
        return response;
    }

    @Override
    public Response<Long> append(byte[] key, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.append(key, value));
        return response;
    }

    @Override
    public Response<List<String>> blpop(String... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<byte[]>> blpop(byte[] ... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<String>> brpop(String... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<byte[]>> brpop(byte[] ... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> decr(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.decr(key));
        return response;
    }

    @Override
    public Response<Long> decr(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.decr(key));
        return response;
    }

    @Override
    public Response<Long> decrBy(String key, long integer) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.decrBy(key, integer));
        return response;
    }

    @Override
    public Response<Long> decrBy(byte[] key, long integer) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.decrBy(key, integer));
        return response;
    }

    @Override
    public Response<Long> del(String... keys) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.del(keys));
        return response;
    }

    @Override
    public Response<Long> del(byte[] ... keys) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.del(keys));
        return response;
    }

    @Override
    public Response<String> echo(String string) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.echo(string));
        return response;
    }

    @Override
    public Response<byte[]> echo(byte[] string) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.echo(string));
        return response;
    }

    @Override
    public Response<Boolean> exists(String key) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.exists(key));
        return response;
    }

    @Override
    public Response<Boolean> exists(byte[] key) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.exists(key));
        return response;
    }

    @Override
    public Response<Long> expire(String key, int seconds) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.expire(key, seconds));
        return response;
    }

    @Override
    public Response<Long> expire(byte[] key, int seconds) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.expire(key, seconds));
        return response;
    }

    @Override
    public Response<Long> expireAt(String key, long unixTime) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.expireAt(key, unixTime));
        return response;
    }

    @Override
    public Response<Long> expireAt(byte[] key, long unixTime) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.expireAt(key, unixTime));
        return response;
    }

    @Override
    public Response<String> get(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.get(key));
        return response;
    }

    @Override
    public Response<byte[]> get(byte[] key) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.get(key));
        return response;
    }

    @Override
    public Response<Boolean> getbit(String key, long offset) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.getbit(key, offset));
        return response;
    }

    @Override
    public Response<String> getrange(String key, long startOffset, long endOffset) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.getrange(key, startOffset, endOffset));
        return response;
    }

    @Override
    public Response<String> getSet(String key, String value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.getSet(key, value));
        return response;
    }

    @Override
    public Response<byte[]> getSet(byte[] key, byte[] value) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.getSet(key, value));
        return response;
    }

    public Response<Long> hdel(String key, String field) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hdel(key, field));
        return response;
    }

    public Response<Long> hdel(byte[] key, byte[] field) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hdel(key, field));
        return response;
    }

    @Override
    public Response<Boolean> hexists(String key, String field) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.hexists(key, field));
        return response;
    }

    @Override
    public Response<Boolean> hexists(byte[] key, byte[] field) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.hexists(key, field));
        return response;
    }

    @Override
    public Response<String> hget(String key, String field) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.hget(key, field));
        return response;
    }

    @Override
    public Response<byte[]> hget(byte[] key, byte[] field) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.hget(key, field));
        return response;
    }

    @Override
    public Response<Map<String, String>> hgetAll(String key) {
        Response<Map<String, String>> response = getResponse(BuilderFactory.STRING_MAP);
        response.set(jedis.hgetAll(key));
        return response;
    }

    @Override
    public Response<Map<byte[], byte[]>> hgetAll(byte[] key) {
        Response<Map<byte[], byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_MAP);
        response.set(jedis.hgetAll(key));
        return response;
    }

    @Override
    public Response<Long> hincrBy(String key, String field, long value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hincrBy(key, field, value));
        return response;
    }

    @Override
    public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hincrBy(key, field, value));
        return response;
    }

    @Override
    public Response<Set<String>> hkeys(String key) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.hkeys(key));
        return response;
    }

    @Override
    public Response<Set<byte[]>> hkeys(byte[] key) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.hkeys(key));
        return response;
    }

    @Override
    public Response<Long> hlen(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hlen(key));
        return response;
    }

    @Override
    public Response<Long> hlen(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hlen(key));
        return response;
    }

    @Override
    public Response<List<String>> hmget(String key, String... fields) {
        Response<List<String>> response = getResponse(BuilderFactory.STRING_LIST);
        response.set(jedis.hmget(key, fields));
        return response;
    }

    @Override
    public Response<List<byte[]>> hmget(byte[] key, byte[] ... fields) {
        Response<List<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_LIST);
        response.set(jedis.hmget(key, fields));
        return response;
    }

    @Override
    public Response<String> hmset(String key, Map<String, String> hash) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.hmset(key, hash));
        return response;
    }

    @Override
    public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.hmset(key, hash));
        return response;
    }

    @Override
    public Response<Long> hset(String key, String field, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hset(key, field, value));
        return response;
    }

    @Override
    public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hset(key, field, value));
        return response;
    }

    @Override
    public Response<Long> hsetnx(String key, String field, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hsetnx(key, field, value));
        return response;
    }

    @Override
    public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.hsetnx(key, field, value));
        return response;
    }

    @Override
    public Response<List<String>> hvals(String key) {
        Response<List<String>> response = getResponse(BuilderFactory.STRING_LIST);
        response.set(jedis.hvals(key));
        return response;
    }

    @Override
    public Response<List<byte[]>> hvals(byte[] key) {
        Response<List<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_LIST);
        response.set(jedis.hvals(key));
        return response;
    }

    @Override
    public Response<Long> incr(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.incr(key));
        return response;
    }

    @Override
    public Response<Long> incr(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.incr(key));
        return response;
    }

    @Override
    public Response<Long> incrBy(String key, long integer) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.incrBy(key, integer));
        return response;
    }

    @Override
    public Response<Long> incrBy(byte[] key, long integer) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.incrBy(key, integer));
        return response;
    }

    @Override
    public Response<Set<String>> keys(String pattern) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.keys(pattern));
        return response;
    }

    @Override
    public Response<Set<byte[]>> keys(byte[] pattern) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.keys(pattern));
        return response;
    }

    public Response<String> lindex(String key, int index) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.lindex(key, index));
        return response;
    }

    public Response<String> lindex(byte[] key, int index) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.lindex(key, index));
        return response;
    }

    @Override
    public Response<Long> linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.linsert(key, where, pivot, value));
        return response;
    }

    @Override
    public Response<Long> linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.linsert(key, where, pivot, value));
        return response;
    }

    @Override
    public Response<Long> llen(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.llen(key));
        return response;
    }

    @Override
    public Response<Long> llen(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.llen(key));
        return response;
    }

    @Override
    public Response<String> lpop(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.lpop(key));
        return response;
    }

    @Override
    public Response<byte[]> lpop(byte[] key) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.lpop(key));
        return response;
    }

    public Response<Long> lpush(String key, String string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lpush(key, string));
        return response;
    }

    public Response<Long> lpush(byte[] key, byte[] string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lpush(key, string));
        return response;
    }

    public Response<Long> lpushx(String key, String string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lpushx(key, string));
        return response;
    }

    public Response<Long> lpushx(byte[] key, byte[] bytes) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lpushx(key, bytes));
        return response;
    }

    @Override
    public Response<List<String>> lrange(String key, long start, long end) {
        Response<List<String>> response = getResponse(BuilderFactory.STRING_LIST);
        response.set(jedis.lrange(key, start, end));
        return response;
    }

    @Override
    public Response<List<byte[]>> lrange(byte[] key, long start, long end) {
        Response<List<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_LIST);
        response.set(jedis.lrange(key, (int) start, (int) end));
        return response;
    }

    @Override
    public Response<Long> lrem(String key, long count, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lrem(key, count, value));
        return response;
    }

    @Override
    public Response<Long> lrem(byte[] key, long count, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lrem(key, (int) count, value));
        return response;
    }

    @Override
    public Response<String> lset(String key, long index, String value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.lset(key, index, value));
        return response;
    }

    @Override
    public Response<String> lset(byte[] key, long index, byte[] value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.lset(key, (int) index, value));
        return response;
    }

    @Override
    public Response<String> ltrim(String key, long start, long end) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.ltrim(key, start, end));
        return response;
    }

    @Override
    public Response<String> ltrim(byte[] key, long start, long end) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.ltrim(key, (int) start, (int) end));
        return response;
    }

    @Override
    public Response<List<String>> mget(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<byte[]>> mget(byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> move(String key, int dbIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> move(byte[] key, int dbIndex) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.move(key, dbIndex));
        return response;
    }

    @Override
    public Response<String> mset(String... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> mset(byte[] ... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> msetnx(String... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> msetnx(byte[] ... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> persist(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.persist(key));
        return response;
    }

    @Override
    public Response<Long> persist(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.persist(key));
        return response;
    }

    @Override
    public Response<String> rename(String oldkey, String newkey) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.rename(oldkey, newkey));
        return response;
    }

    @Override
    public Response<String> rename(byte[] oldkey, byte[] newkey) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.rename(oldkey, newkey));
        return response;
    }

    @Override
    public Response<Long> renamenx(String oldkey, String newkey) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.renamenx(oldkey, newkey));
        return response;
    }

    @Override
    public Response<Long> renamenx(byte[] oldkey, byte[] newkey) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.renamenx(oldkey, newkey));
        return response;
    }

    @Override
    public Response<String> rpop(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.rpop(key));
        return response;
    }

    @Override
    public Response<byte[]> rpop(byte[] key) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.rpop(key));
        return response;
    }

    @Override
    public Response<String> rpoplpush(String srckey, String dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<byte[]> rpoplpush(byte[] srckey, byte[] dstkey) {
        throw new UnsupportedOperationException();
    }

    public Response<Long> rpush(String key, String string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.rpush(key, string));
        return response;
    }

    public Response<Long> rpush(byte[] key, byte[] string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.rpush(key, string));
        return response;
    }

    public Response<Long> rpushx(String key, String string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.rpushx(key, string));
        return response;
    }

    public Response<Long> rpushx(byte[] key, byte[] string) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.rpushx(key, string));
        return response;
    }

    public Response<Long> sadd(String key, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.sadd(key, member));
        return response;
    }

    public Response<Long> sadd(byte[] key, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.sadd(key, member));
        return response;
    }

    @Override
    public Response<Long> scard(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.scard(key));
        return response;
    }

    @Override
    public Response<Long> scard(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.scard(key));
        return response;
    }

    @Override
    public Response<Set<String>> sdiff(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> sdiff(byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sdiffstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sdiffstore(byte[] dstkey, byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> set(String key, String value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.set(key, value));
        return response;
    }

    @Override
    public Response<String> set(byte[] key, byte[] value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.set(key, value));
        return response;
    }

    @Override
    public Response<Boolean> setbit(String key, long offset, boolean value) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.setbit(key, offset, value));
        return response;
    }

    @Override
    public Response<String> setex(String key, int seconds, String value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.setex(key, seconds, value));
        return response;
    }

    @Override
    public Response<String> setex(byte[] key, int seconds, byte[] value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.setex(key, seconds, value));
        return response;
    }

    @Override
    public Response<Long> setnx(String key, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.setnx(key, value));
        return response;
    }

    @Override
    public Response<Long> setnx(byte[] key, byte[] value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.setnx(key, value));
        return response;
    }

    @Override
    public Response<Long> setrange(String key, long offset, String value) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.setrange(key, offset, value));
        return response;
    }

    @Override
    public Response<Set<String>> sinter(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> sinter(byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sinterstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sinterstore(byte[] dstkey, byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Boolean> sismember(String key, String member) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.sismember(key, member));
        return response;
    }

    @Override
    public Response<Boolean> sismember(byte[] key, byte[] member) {
        Response<Boolean> response = getResponse(BuilderFactory.BOOLEAN);
        response.set(jedis.sismember(key, member));
        return response;
    }

    @Override
    public Response<Set<String>> smembers(String key) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.smembers(key));
        return response;
    }

    @Override
    public Response<Set<byte[]>> smembers(byte[] key) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.smembers(key));
        return response;
    }

    @Override
    public Response<Long> smove(String srckey, String dstkey, String member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> smove(byte[] srckey, byte[] dstkey, byte[] member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<String>> sort(String key) {
        Response<List<String>> response = getResponse(BuilderFactory.STRING_LIST);
        response.set(jedis.sort(key));
        return response;
    }

    @Override
    public Response<List<byte[]>> sort(byte[] key) {
        Response<List<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_LIST);
        response.set(jedis.sort(key));
        return response;
    }

    @Override
    public Response<List<String>> sort(String key, SortingParams sortingParameters) {
        Response<List<String>> response = getResponse(BuilderFactory.STRING_LIST);
        response.set(jedis.sort(key, sortingParameters));
        return response;
    }

    @Override
    public Response<List<byte[]>> sort(byte[] key, SortingParams sortingParameters) {
        Response<List<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_LIST);
        response.set(jedis.sort(key, sortingParameters));
        return response;
    }

    @Override
    public Response<Long> sort(String key, SortingParams sortingParameters, String dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sort(String key, String dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sort(byte[] key, byte[] dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> spop(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.spop(key));
        return response;
    }

    @Override
    public Response<byte[]> spop(byte[] key) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.spop(key));
        return response;
    }

    @Override
    public Response<String> srandmember(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.srandmember(key));
        return response;
    }

    @Override
    public Response<byte[]> srandmember(byte[] key) {
        Response<byte[]> response = getResponse(BuilderFactory.BYTE_ARRAY);
        response.set(jedis.srandmember(key));
        return response;
    }

    public Response<Long> srem(String key, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.srem(key, member));
        return response;
    }

    public Response<Long> srem(byte[] key, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.srem(key, member));
        return response;
    }

    @Override
    public Response<Long> strlen(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> strlen(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> substr(String key, int start, int end) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.substr(key, start, end));
        return response;
    }

    @Override
    public Response<String> substr(byte[] key, int start, int end) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.substr(key, start, end));
        return response;
    }

    @Override
    public Response<Set<String>> sunion(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> sunion(byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sunionstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> sunionstore(byte[] dstkey, byte[] ... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> ttl(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.ttl(key));
        return response;
    }

    @Override
    public Response<Long> ttl(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.ttl(key));
        return response;
    }

    @Override
    public Response<String> type(String key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.type(key));
        return response;
    }

    @Override
    public Response<String> type(byte[] key) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.type(key));
        return response;
    }

    @Override
    public Response<String> watch(String... keys) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.watch(keys));
        return response;
    }

    @Override
    public Response<String> watch(byte[] ... keys) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.watch(keys));
        return response;
    }

    @Override
    public Response<Long> zadd(String key, double score, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zadd(key, score, member));
        return response;
    }

    @Override
    public Response<Long> zadd(byte[] key, double score, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zadd(key, score, member));
        return response;
    }

    @Override
    public Response<Long> zcard(String key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zcard(key));
        return response;
    }

    @Override
    public Response<Long> zcard(byte[] key) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zcard(key));
        return response;
    }

    @Override
    public Response<Long> zcount(String key, double min, double max) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zcount(key, min, max));
        return response;
    }

    @Override
    public Response<Long> zcount(byte[] key, double min, double max) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zcount(key, min, max));
        return response;
    }

    @Override
    public Response<Double> zincrby(String key, double score, String member) {
        Response<Double> response = getResponse(BuilderFactory.DOUBLE);
        response.set(jedis.zincrby(key, score, member));
        return response;
    }

    @Override
    public Response<Double> zincrby(byte[] key, double score, byte[] member) {
        Response<Double> response = getResponse(BuilderFactory.DOUBLE);
        response.set(jedis.zincrby(key, score, member));
        return response;
    }

    @Override
    public Response<Long> zinterstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zinterstore(byte[] dstkey, byte[] ... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zinterstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zinterstore(byte[] dstkey, ZParams params, byte[] ... sets) {
        throw new UnsupportedOperationException();
    }

    public Response<Set<String>> zrange(String key, int start, int end) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrange(key, start, end));
        return response;
    }

    public Response<Set<String>> zrange(byte[] key, int start, int end) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrange(key, start, end));
        return response;
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, double min, double max) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrangeByScore(key, min, max));
        return response;
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.zrangeByScore(key, min, max));
        return response;
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, String min, String max) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrangeByScore(key, min, max));
        return response;
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.zrangeByScore(key, min, max));
        return response;
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, double min, double max, int offset, int count) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrangeByScore(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.zrangeByScore(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.zrangeByScore(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        return response;
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, double max, double min) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrevrange(key, (long) max, (long) min));
        return response;
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min) {
        Response<Set<byte[]>> response = getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
        response.set(jedis.zrevrange(key, (int) max, (int) min));
        return response;
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, String max, String min) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        return response;
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        return response;
    }

    public Response<Set<Tuple>> zrangeWithScores(String key, int start, int end) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeWithScores(key, (long) start, (long) end));
        return response;
    }
    
    public Response<Set<Tuple>> zrangeWithScores(byte[] key, int start, int end) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrangeWithScores(key, start, end));
        return response;
    }

    @Override
    public Response<Long> zrank(String key, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrank(key, member));
        return response;
    }

    @Override
    public Response<Long> zrank(byte[] key, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrank(key, member));
        return response;
    }

    public Response<Long> zrem(String key, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrem(key, member));
        return response;
    }

    public Response<Long> zrem(byte[] key, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrem(key, member));
        return response;
    }

    public Response<Long> zremrangeByRank(String key, int start, int end) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zremrangeByRank(key, start, end));
        return response;
    }

    public Response<Long> zremrangeByRank(byte[] key, int start, int end) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zremrangeByRank(key, start, end));
        return response;
    }

    @Override
    public Response<Long> zremrangeByScore(String key, double start, double end) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zremrangeByScore(key, start, end));
        return response;
    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zremrangeByScore(key, start, end));
        return response;
    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zremrangeByScore(key, start, end));
        return response;
    }

    public Response<Set<String>> zrevrange(String key, int start, int end) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrevrange(key, start, end));
        return response;
    }

    public Response<Set<String>> zrevrange(byte[] key, int start, int end) {
        Response<Set<String>> response = getResponse(BuilderFactory.STRING_SET);
        response.set(jedis.zrevrange(key, start, end));
        return response;
    }

    public Response<Set<Tuple>> zrevrangeWithScores(String key, int start, int end) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeWithScores(key, start, end));
        return response;
    }

    public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, int start, int end) {
        Response<Set<Tuple>> response = getResponse(BuilderFactory.TUPLE_ZSET);
        response.set(jedis.zrevrangeWithScores(key, start, end));
        return response;
    }

    @Override
    public Response<Long> zrevrank(String key, String member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrevrank(key, member));
        return response;
    }

    @Override
    public Response<Long> zrevrank(byte[] key, byte[] member) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.zrevrank(key, member));
        return response;
    }

    @Override
   public  Response<Double> zscore(String key, String member) {
        Response<Double> response = getResponse(BuilderFactory.DOUBLE);
        response.set(jedis.zscore(key, member));
        return response;
    }

    @Override
    public Response<Double> zscore(byte[] key, byte[] member) {
        Response<Double> response = getResponse(BuilderFactory.DOUBLE);
        response.set(jedis.zscore(key, member));
        return response;
    }

    @Override
    public Response<Long> zunionstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zunionstore(byte[] dstkey, byte[] ... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zunionstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> zunionstore(byte[] dstkey, ZParams params, byte[] ... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> bgrewriteaof() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.bgrewriteaof());
        return response;
    }
    
    @Override
    public Response<String> bgsave() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.bgsave());
        return response;
    }

    @Override
    public Response<String> configGet(String pattern) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.configGet(pattern));
        return response;
    }

    @Override
    public Response<String> configSet(String parameter, String value) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.configSet(parameter, value));
        return response;
    }

    @Override
    public Response<String> brpoplpush(String source, String destination, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<byte[]> brpoplpush(byte[] source, byte[] destination, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> configResetStat() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.configResetStat());
        return response;
    }

    @Override
    public Response<String> save() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.save());
        return response;
    }

    @Override
    public Response<Long> lastsave() {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.lastsave());
        return response;
    }

    @Override
    public Response<String> discard() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<List<Object>> exec() {
        Response<List<Object>> response = super.getResponse(currentMulti);
        currentMulti = null;
        return response;
    }

    @Override
    public Response<String> multi() {
        Response<String> response = getResponse(BuilderFactory.STRING); //Expecting OK
        currentMulti = new MultiResponseBuilder();
        return response;
    }

    @Override
    public Response<Long> publish(String channel, String message) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.publish(channel, message));
        return response;
    }

    @Override
    public Response<Long> publish(byte[] channel, byte[] message) {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.publish(channel, message));
        return response;
    }
    
    @Override
    public Response<String> flushDB() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.flushDB());
        return response;
    }

    @Override
    public Response<String> flushAll() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.flushAll());
        return response;
    }

    @Override
    public Response<String> info() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.info());
        return response;
    }

    @Override
    public Response<Long> dbSize() {
        Response<Long> response = getResponse(BuilderFactory.LONG);
        response.set(jedis.dbSize());
        return response;
    }

    @Override
    public Response<String> shutdown() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.shutdown());
        return response;
    }

    @Override
    public Response<String> ping() {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.ping());
        return response;
    }

    @Override
    public Response<String> randomKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<String> select(int index) {
        Response<String> response = getResponse(BuilderFactory.STRING);
        response.set(jedis.select(index));
        return response;
    }

    @Override
    public void sync() {
    }

    @Override
    public List<Object> syncAndReturnAll() {
        return Collections.EMPTY_LIST;
    }
    
    @Override
    protected <T> Response<T> getResponse(Builder<T> builder) {
        if (currentMulti != null){
            super.getResponse(BuilderFactory.STRING); //Expected QUEUED

            Response<T> lr = new Response<T>(builder);
            currentMulti.addResponse(lr);
            return lr;
        }
        return super.getResponse(builder);
    }

    private class MultiResponseBuilder extends Builder<List<Object>> {
        private final List<Response<?>> responses = new ArrayList<Response<?>>();

        @Override
        public List<Object> build(Object data) {
            List<Object> list = (List<Object>) data;
            List<Object> values = new ArrayList<Object>();

            if (list.size() != responses.size()) {
                throw new JedisDataException("Expected data size "
                        + responses.size() + " but was " + list.size());
            }

            for (int i = 0; i < list.size(); i++) {
                Response<?> response = responses.get(i);
                response.set(list.get(i));
                values.add(response.get());
            }
            return values;
        }

        public void addResponse(Response<?> response) {
            responses.add(response);
        }
    }

}
