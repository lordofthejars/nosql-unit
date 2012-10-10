package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.convert;
import static ch.lambdaj.Lambda.filter;
import static com.lordofthejars.nosqlunit.redis.embedded.MatchesGlobRegexpMatcher.matches;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.SortingParams;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Slowlog;

import com.lordofthejars.nosqlunit.redis.embedded.ExpirationDatatypeOperations.TtlState;

public class KeysServerOperations {

	protected static final String NONE = "none";
	private static final String OK = "OK";
	private static final String KO = "-";
	private List<RedisDatatypeOperations> redisDatatypeOperations;

	private KeysServerOperations() {
		super();
	}

	public static KeysServerOperations createKeysServerOperations(RedisDatatypeOperations... redisDatatypeOperations) {

		KeysServerOperations keysServerOperations = new KeysServerOperations();

		keysServerOperations.redisDatatypeOperations = Arrays.asList(redisDatatypeOperations);

		return keysServerOperations;
	}

	/**
	 * Return the number of keys in the currently selected database.
	 * 
	 * @return Integer reply
	 */
	public Long dbSize() {

		long numberOfKeys = 0L;

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			numberOfKeys += redisDatatypeOperations.getNumberOfKeys();
		}

		return numberOfKeys;
	}

	/**
	 * Delete all the keys of all the existing databases, not just the currently
	 * selected one. This command never fails.
	 * 
	 * @return Status code reply
	 */
	public String flushAll() {
		return flushDB();
	}

	/**
	 * Delete all the keys of the currently selected DB. This command never
	 * fails.
	 * 
	 * @return Status code reply
	 */
	public String flushDB() {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			redisDatatypeOperations.flushAllKeys();
		}

		return OK;

	}

	/**
	 * Remove the specified keys. If a given key does not exist no operation is
	 * performed for this key. The command returns the number of keys removed.
	 * 
	 * Time complexity: O(1)
	 * 
	 * @param keys
	 * @return Integer reply, specifically: an integer greater than 0 if one or
	 *         more keys were removed 0 if none of the specified key existed
	 */
	public Long del(final byte[]... keys) {

		long numberOfRemovedelements = 0;

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			numberOfRemovedelements += redisDatatypeOperations.del(keys);
		}

		return numberOfRemovedelements;

	}

	/**
	 * Test if the specified key exists. The command returns "1" if the key
	 * exists, otherwise "0" is returned. Note that even keys set with an empty
	 * string as value will return "1".
	 * 
	 * Time complexity: O(1)
	 * 
	 * @param key
	 * @return Boolean reply, true if the key exists, otherwise false
	 */
	public Boolean exists(final byte[] key) {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			if (redisDatatypeOperations.exists(key)) {
				return true;
			}
		}

		return false;

	}

	private RedisDatatypeOperations whereIsKey(byte[] key) {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			if (redisDatatypeOperations.exists(key)) {
				return redisDatatypeOperations;
			}
		}

		return null;
	}

	/**
	 * Atomically renames the key oldkey to newkey. If the source and
	 * destination name are the same an error is returned. If newkey already
	 * exists it is overwritten.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @param oldKey
	 * @param newKey
	 * @return Status code repy
	 */
	public String rename(final byte[] oldKey, final byte[] newKey) {

		if (Arrays.equals(oldKey, newKey)) {
			return KO;
		}

		if (exists(oldKey)) {

			RedisDatatypeOperations newKeyRepository = whereIsKey(newKey);

			if (newKeyRepository != null) {
				newKeyRepository.del(newKey);
			}

			RedisDatatypeOperations oldKeyRepository = whereIsKey(oldKey);
			oldKeyRepository.renameKey(oldKey, newKey);

			return OK;
		}

		return KO;
	}

	/**
	 * Rename oldkey into newkey but fails if the destination key newkey already
	 * exists.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @param oldkey
	 * @param newkey
	 * @return Integer reply, specifically: 1 if the key was renamed 0 if the
	 *         target key already exist
	 */
	public Long renamenx(final byte[] oldkey, final byte[] newkey) {
		if (!exists(newkey)) {
			rename(oldkey, newkey);
			return 1L;
		}

		return 0L;
	}

	/**
	 * Set a timeout on the specified key. After the timeout the key will be
	 * automatically deleted by the server. A key with an associated timeout is
	 * said to be volatile in Redis terminology.
	 * <p>
	 * Voltile keys are stored on disk like the other keys, the timeout is
	 * persistent too like all the other aspects of the dataset. Saving a
	 * dataset containing expires and stopping the server does not stop the flow
	 * of time as Redis stores on disk the time when the key will no longer be
	 * available as Unix time, and not the remaining seconds.
	 * <p>
	 * Since Redis 2.1.3 you can update the value of the timeout of a key
	 * already having an expire set. It is also possible to undo the expire at
	 * all turning the key into a normal key using the {@link #persist(String)
	 * PERSIST} command.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
	 * 
	 * @param key
	 * @param seconds
	 * @return Integer reply, specifically: 1: the timeout was set. 0: the
	 *         timeout was not set since the key already has an associated
	 *         timeout (this may happen only in Redis versions < 2.1.3, Redis >=
	 *         2.1.3 will happily update the timeout), or the key does not
	 *         exist.
	 */
	public Long expire(final byte[] key, final int seconds) {

		RedisDatatypeOperations redisOperation = whereIsKey(key);

		if (redisOperation != null) {
			redisOperation.addExpirationTime(key, seconds, TimeUnit.SECONDS);
			return 1L;
		} else {
			return 0L;
		}

	}

	/**
	 * EXPIREAT works exctly like {@link #expire(String, int) EXPIRE} but
	 * instead to get the number of seconds representing the Time To Live of the
	 * key as a second argument (that is a relative way of specifing the TTL),
	 * it takes an absolute one in the form of a UNIX timestamp (Number of
	 * seconds elapsed since 1 Gen 1970).
	 * <p>
	 * EXPIREAT was introduced in order to implement the Append Only File
	 * persistence mode so that EXPIRE commands are automatically translated
	 * into EXPIREAT commands for the append only file. Of course EXPIREAT can
	 * also used by programmers that need a way to simply specify that a given
	 * key should expire at a given time in the future.
	 * <p>
	 * Since Redis 2.1.3 you can update the value of the timeout of a key
	 * already having an expire set. It is also possible to undo the expire at
	 * all turning the key into a normal key using the {@link #persist(String)
	 * PERSIST} command.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
	 * 
	 * @param key
	 * @param unixTime
	 * @return Integer reply, specifically: 1: the timeout was set. 0: the
	 *         timeout was not set since the key already has an associated
	 *         timeout (this may happen only in Redis versions < 2.1.3, Redis >=
	 *         2.1.3 will happily update the timeout), or the key does not
	 *         exist.
	 */
	public Long expireAt(final byte[] key, final long unixTime) {

		RedisDatatypeOperations redisOperation = whereIsKey(key);

		if (redisOperation != null) {
			redisOperation.addExpirationAt(key, unixTime, TimeUnit.SECONDS);
			return 1L;
		} else {
			return 0L;
		}

	}

	/**
	 * Returns all the keys matching the glob-style pattern as space separated
	 * strings. For example if you have in the database the keys "foo" and
	 * "foobar" the command "KEYS foo*" will return "foo foobar".
	 * <p>
	 * Note that while the time complexity for this operation is O(n) the
	 * constant times are pretty low. For example Redis running on an entry
	 * level laptop can scan a 1 million keys database in 40 milliseconds.
	 * <b>Still it's better to consider this one of the slow commands that may
	 * ruin the DB performance if not used with care.</b>
	 * <p>
	 * In other words this command is intended only for debugging and special
	 * operations like creating a script to change the DB schema. Don't use it
	 * in your normal code. Use Redis Sets in order to group together a subset
	 * of objects.
	 * <p>
	 * Glob style patterns examples:
	 * <ul>
	 * <li>h?llo will match hello hallo hhllo
	 * <li>h*llo will match hllo heeeello
	 * <li>h[ae]llo will match hello and hallo, but not hillo
	 * </ul>
	 * <p>
	 * Use \ to escape special chars if you want to match them verbatim.
	 * <p>
	 * 
	 * @param pattern
	 * @return Multi bulk reply
	 */
	public Set<byte[]> keys(final byte[] patternbyte) {

		String pattern = SafeEncoder.encode(patternbyte);

		List<String> stringKeys = new ArrayList<String>();

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			stringKeys.addAll(convert(redisDatatypeOperations.keys(),
					ByteArrayToStringConverter.createByteArrayToStringConverter()));
		}

		List<String> filteredKeys = filter(matches(pattern), stringKeys);
		return new HashSet<byte[]>(convert(filteredKeys, StringToByteArrayConverter.createStringToByteArrayConverter()));
	}

	/**
	 * Undo a {@link #expire(String, int) expire} at turning the expire key into
	 * a normal key.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @param key
	 * @return Integer reply, specifically: 1: the key is now persist. 0: the
	 *         key is not persist (only happens when key not set).
	 */
	public Long persist(final byte[] key) {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			if (redisDatatypeOperations.removeExpiration(key)) {
				return 1L;
			}
		}

		return 0L;

	}

	/**
	 * The TTL command returns the remaining time to live in seconds of a key
	 * that has an {@link #expire(String, int) EXPIRE} set. This introspection
	 * capability allows a Redis client to check how many seconds a given key
	 * will continue to be part of the dataset.
	 * 
	 * @param key
	 * @return Integer reply, returns the remaining time to live in seconds of a
	 *         key that has an EXPIRE. If the Key does not exists or does not
	 *         have an associated expire, -1 is returned.
	 */
	public Long ttl(final byte[] key) {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			long ttlTime = redisDatatypeOperations.remainingTime(key);

			if (ttlTime != ExpirationDatatypeOperations.NO_EXPIRATION) {
				return ttlTime;
			}

		}

		return ExpirationDatatypeOperations.NO_EXPIRATION;
	}

	/**
	 * Return the type of the value stored at key in form of a string. The type
	 * can be one of "none", "string", "list", "set". "none" is returned if the
	 * key does not exist.
	 * 
	 * Time complexity: O(1)
	 * 
	 * @param key
	 * @return Status code reply, specifically: "none" if the key does not exist
	 *         "string" if the key contains a String value "list" if the key
	 *         contains a List value "set" if the key contains a Set value
	 *         "zset" if the key contains a Sorted Set value "hash" if the key
	 *         contains a Hash value
	 */
	public String type(final byte[] key) {

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			if (redisDatatypeOperations.exists(key)) {
				return redisDatatypeOperations.type();
			}
		}

		return NONE;
	}

	/**
	 * Sort a Set or a List.
	 * <p>
	 * Sort the elements contained in the List, Set, or Sorted Set value at key.
	 * By default sorting is numeric with elements being compared as double
	 * precision floating point numbers. This is the simplest form of SORT.
	 * 
	 * @see #sort(String, String)
	 * @see #sort(String, SortingParams)
	 * @see #sort(String, SortingParams, String)
	 * 
	 * 
	 * @param key
	 * @return Assuming the Set/List at key contains a list of numbers, the
	 *         return value will be the list of numbers ordered from the
	 *         smallest to the biggest number.
	 */
	public List<byte[]> sort(final byte[] key) {

		if (this.type(key).equals(HashDatatypeOperations.HASH)
				|| this.type(key).equals(StringDatatypeOperations.STRING)) {
			throw new UnsupportedOperationException("ERR Operation against a key holding the wrong kind of value");
		}

		RedisDatatypeOperations datastoreWithKey = whereIsKey(key);

		if (datastoreWithKey != null) {
			return datastoreWithKey.sort(key);
		}

		return Collections.EMPTY_LIST;
	}

	public void updateTtl() {
		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {

			List<byte[]> keys = redisDatatypeOperations.keys();
			for (byte[] key : keys) {
				updateTtl(key);
			}
		}
	}

	public void updateTtl(byte[] key) {
		RedisDatatypeOperations datastore = whereIsKey(key);

		if (datastore != null) {
			TtlState ttlState = datastore.timedoutState(key);
			if (ttlState == TtlState.EXPIRED) {
				this.del(key);
			}
		}

	}

	public Long move(final byte[] key, final int dbIndex) {
		return 1L;
	}

	public Long objectRefcount(byte[] string) {
		throw new UnsupportedOperationException("Object Ref is not supported.");
	}

	public byte[] objectEncoding(byte[] string) {
		throw new UnsupportedOperationException("Object Encoding is not supported.");
	}

	public Long objectIdletime(byte[] string) {
		throw new UnsupportedOperationException("Object Idel is not supported.");
	}

	/**
	 * Return a randomly selected key from the currently selected DB.
	 * <p>
	 * Time complexity: O(1)
	 * 
	 * @return Singe line reply, specifically the randomly selected key or an
	 *         empty string is the database is empty
	 */
	public byte[] randomKey() {

		List<byte[]> allKeys = new ArrayList<byte[]>();

		for (RedisDatatypeOperations redisDatatypeOperations : this.redisDatatypeOperations) {
			allKeys.addAll(redisDatatypeOperations.keys());
		}

		if (allKeys.size() > 0) {
			int randomIndex = generateRandomIndex(allKeys);
			return allKeys.get(randomIndex);
		}

		return null;

	}

	public String bgrewriteaof() {
		return OK;
	}

	public String save() {
		return OK;
	}

	public String bgsave() {
		return OK;
	}

	/**
	 * Retrieve the configuration of a running Redis server. Not all the
	 * configuration parameters are supported.
	 * <p>
	 * CONFIG GET returns the current configuration parameters. This sub command
	 * only accepts a single argument, that is glob style pattern. All the
	 * configuration parameters matching this parameter are reported as a list
	 * of key-value pairs.
	 * <p>
	 * <b>Example:</b>
	 * 
	 * <pre>
	 * $ redis-cli config get '*'
	 * 1. "dbfilename"
	 * 2. "dump.rdb"
	 * 3. "requirepass"
	 * 4. (nil)
	 * 5. "masterauth"
	 * 6. (nil)
	 * 7. "maxmemory"
	 * 8. "0\n"
	 * 9. "appendfsync"
	 * 10. "everysec"
	 * 11. "save"
	 * 12. "3600 1 300 100 60 10000"
	 * 
	 * $ redis-cli config get 'm*'
	 * 1. "masterauth"
	 * 2. (nil)
	 * 3. "maxmemory"
	 * 4. "0\n"
	 * </pre>
	 * 
	 * @param pattern
	 * @return Bulk reply.
	 */
	public List<byte[]> configGet(final byte[] pattern) {
		return new ArrayList<byte[]>();
	}


	/**
	 * Alter the configuration of a running Redis server. Not all the
	 * configuration parameters are supported.
	 * <p>
	 * The list of configuration parameters supported by CONFIG SET can be
	 * obtained issuing a {@link #configGet(String) CONFIG GET *} command.
	 * <p>
	 * The configuration set using CONFIG SET is immediately loaded by the Redis
	 * server that will start acting as specified starting from the next
	 * command.
	 * <p>
	 * 
	 * <b>Parameters value format</b>
	 * <p>
	 * The value of the configuration parameter is the same as the one of the
	 * same parameter in the Redis configuration file, with the following
	 * exceptions:
	 * <p>
	 * <ul>
	 * <li>The save paramter is a list of space-separated integers. Every pair
	 * of integers specify the time and number of changes limit to trigger a
	 * save. For instance the command CONFIG SET save "3600 10 60 10000" will
	 * configure the server to issue a background saving of the RDB file every
	 * 3600 seconds if there are at least 10 changes in the dataset, and every
	 * 60 seconds if there are at least 10000 changes. To completely disable
	 * automatic snapshots just set the parameter as an empty string.
	 * <li>All the integer parameters representing memory are returned and
	 * accepted only using bytes as unit.
	 * </ul>
	 * 
	 * @param parameter
	 * @param value
	 * @return Status code reply
	 */
	public byte[] configSet(final byte[] parameter, final byte[] value) {
		return "OK".getBytes();
	}

	/**
	 * Reset the stats returned by INFO
	 * 
	 * @return
	 */
	public String configResetStat() {
		return "OK";
	}

	/**
	 * Provide information and statistics about the server.
	 * <p>
	 * The info command returns different information and statistics about the
	 * server in an format that's simple to parse by computers and easy to read
	 * by humans.
	 * <p>
	 * <b>Format of the returned String:</b>
	 * <p>
	 * All the fields are in the form field:value
	 * 
	 * <pre>
	 * edis_version:0.07
	 * connected_clients:1
	 * connected_slaves:0
	 * used_memory:3187
	 * changes_since_last_save:0
	 * last_save_time:1237655729
	 * total_connections_received:1
	 * total_commands_processed:1
	 * uptime_in_seconds:25
	 * uptime_in_days:0
	 * </pre>
	 * 
	 * <b>Notes</b>
	 * <p>
	 * used_memory is returned in bytes, and is the total number of bytes
	 * allocated by the program using malloc.
	 * <p>
	 * uptime_in_days is redundant since the uptime in seconds contains already
	 * the full uptime information, this field is only mainly present for
	 * humans.
	 * <p>
	 * changes_since_last_save does not refer to the number of key changes, but
	 * to the number of operations that produced some kind of change in the
	 * dataset.
	 * <p>
	 * 
	 * @return Bulk reply
	 */
	public String info() {
		return "Embedded Jedis";
	}

	/**
	 * Return the UNIX time stamp of the last successfully saving of the dataset
	 * on disk.
	 * <p>
	 * Return the UNIX TIME of the last DB save executed with success. A client
	 * may check if a {@link #bgsave() BGSAVE} command succeeded reading the
	 * LASTSAVE value, then issuing a BGSAVE command and checking at regular
	 * intervals every N seconds if LASTSAVE changed.
	 * 
	 * @return Integer reply, specifically an UNIX time stamp.
	 */
	public Long lastsave() {
		return System.currentTimeMillis();
	}

	/**
	 * Dump all the received requests in real time.
	 * <p>
	 * MONITOR is a debugging command that outputs the whole sequence of
	 * commands received by the Redis server. is very handy in order to
	 * understand what is happening into the database. This command is used
	 * directly via telnet.
	 * 
	 * @param jedisMonitor
	 */
	public void monitor(final JedisMonitor jedisMonitor) {

	}

	/**
	 * Synchronously save the DB on disk, then shutdown the server.
	 * <p>
	 * Stop all the clients, save the DB, then quit the server. This commands
	 * makes sure that the DB is switched off without the lost of any data. This
	 * is not guaranteed if the client uses simply {@link #save() SAVE} and then
	 * {@link #quit() QUIT} because other clients may alter the DB data between
	 * the two commands.
	 * 
	 * @return Status code reply on error. On success nothing is returned since
	 *         the server quits and the connection is closed.
	 */
	public String shutdown() {
		return OK;
	}

	/**
	 * Change the replication settings.
	 * <p>
	 * The SLAVEOF command can change the replication settings of a slave on the
	 * fly. If a Redis server is arleady acting as slave, the command SLAVEOF NO
	 * ONE will turn off the replicaiton turning the Redis server into a MASTER.
	 * In the proper form SLAVEOF hostname port will make the server a slave of
	 * the specific server listening at the specified hostname and port.
	 * <p>
	 * If a server is already a slave of some master, SLAVEOF hostname port will
	 * stop the replication against the old server and start the
	 * synchrnonization against the new one discarding the old dataset.
	 * <p>
	 * The form SLAVEOF no one will stop replication turning the server into a
	 * MASTER but will not discard the replication. So if the old master stop
	 * working it is possible to turn the slave into a master and set the
	 * application to use the new master in read/write. Later when the other
	 * Redis server will be fixed it can be configured in order to work as
	 * slave.
	 * <p>
	 * 
	 * @param host
	 * @param port
	 * @return Status code reply
	 */
	public String slaveof(final String host, final int port) {
		return OK;
	}

	public String slaveofNoOne() {
		return OK;
	}

	public List<Slowlog> slowlogGet() {
		return new ArrayList<Slowlog>();
	}

	public List<Slowlog> slowlogGet(long entries) {
		return new ArrayList<Slowlog>();
	}

	public byte[] slowlogReset() {
		return "OK".getBytes();
	}

	public long slowlogLen() {
		return 0L;
	}

	public List<byte[]> slowlogGetBinary() {
		return new ArrayList<byte[]>();
	}

	public List<byte[]> slowlogGetBinary(long entries) {
		return new ArrayList<byte[]>();
	}

	public void sync() {
	}
	
	public Long time() {
		return System.currentTimeMillis();
	}
	
	public Long getDB() {
		return 0L;
	}
	
	public boolean isConnected() {
		return true;
	}
	
	private int generateRandomIndex(List<byte[]> allKeys) {
		Random random = new Random();
		int randomIndex = random.nextInt(allKeys.size());
		return randomIndex;
	}

}
