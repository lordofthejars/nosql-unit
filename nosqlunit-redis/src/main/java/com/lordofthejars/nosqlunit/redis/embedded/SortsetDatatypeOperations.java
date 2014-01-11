package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.closure;
import static ch.lambdaj.Lambda.convert;
import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.filter;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.of;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectUnique;
import static ch.lambdaj.Lambda.var;
import static ch.lambdaj.collection.LambdaCollections.with;
import static com.lordofthejars.nosqlunit.redis.embedded.RangeUtils.getRealScoreForMaxValue;
import static com.lordofthejars.nosqlunit.redis.embedded.RangeUtils.getRealScoreForMinValue;
import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static redis.clients.jedis.Protocol.Keyword.AGGREGATE;
import static redis.clients.jedis.Protocol.Keyword.WEIGHTS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.jedis.ZParams;
import redis.clients.util.SafeEncoder;
import ch.lambdaj.function.closure.Closure1;
import ch.lambdaj.function.convert.Converter;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class SortsetDatatypeOperations extends ExpirationDatatypeOperations implements RedisDatatypeOperations {

	protected static final String ZSET = "zset";
	protected Multimap<ByteBuffer, ScoredByteBuffer> sortset = TreeMultimap.create();

	public Long zadd(final byte[] key, final double score, final byte[] member) {

		ByteBuffer wrappedKey = wrap(key);

		if (sortset.containsKey(wrappedKey)) {

			ScoredByteBuffer previousMember = findScoredByteBufferByKeyAndMember(member, wrappedKey);

			if (isMemberAlreadyAdded(previousMember)) {
				removeAndUpdateElement(score, member, wrappedKey, previousMember);
				return 0L;
			}

		}

		sortset.put(wrappedKey, ScoredByteBuffer.createScoredByteBuffer(wrap(member), score));
		return 1L;

	}

	private void removeAndUpdateElement(final double score, final byte[] member, ByteBuffer wrappedKey,
			ScoredByteBuffer previousMember) {
		sortset.remove(wrappedKey, previousMember);
		sortset.put(wrappedKey, ScoredByteBuffer.createScoredByteBuffer(wrap(member), score));
	}

	private ScoredByteBuffer findScoredByteBufferByKeyAndMember(final byte[] member, ByteBuffer wrappedKey) {
		Collection<ScoredByteBuffer> members = sortset.get(wrappedKey);
		ScoredByteBuffer previousMember = selectUnique(members,
				having(on(ScoredByteBuffer.class).getByteBuffer(), equalTo(wrap(member))));
		return previousMember;
	}

	private boolean isMemberAlreadyAdded(ScoredByteBuffer previousMember) {
		return previousMember != null;
	}

	/**
	 * Add the specified member having the specifeid score to the sorted set
	 * stored at key. If member is already a member of the sorted set the score
	 * is updated, and the element reinserted in the right position to ensure
	 * sorting. If key does not exist a new sorted set with the specified member
	 * as sole member is crated. If the key exists but does not hold a sorted
	 * set value an error is returned.
	 * <p>
	 * The score value can be the string representation of a double precision
	 * floating point number.
	 * <p>
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return Integer reply, specifically: 1 if the new element was added 0 if
	 *         the element was already a member of the sorted set and the score
	 *         was updated
	 */
	public Long zadd(final byte[] key, final Map<Double, byte[]> scoreMembers) {

		long insertedElements = 0;

		Set<Entry<Double, byte[]>> scoreMemberSet = scoreMembers.entrySet();

		for (Entry<Double, byte[]> entry : scoreMemberSet) {
			insertedElements += zadd(key, entry.getKey(), entry.getValue());
		}

		return insertedElements;
	}

	/**
	 * Return the sorted set cardinality (number of elements). If the key does
	 * not exist 0 is returned, like for empty sorted sets.
	 * <p>
	 * Time complexity O(1)
	 * 
	 * @param key
	 * @return the cardinality (number of elements) of the set as an integer.
	 */
	public Long zcard(final byte[] key) {
		return (long) sortset.get(wrap(key)).size();
	}

	public Long zcount(final byte[] key, final double min, final double max) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		long numberOfElementsMeetingCondition = countNumberOfElementsBetweenInclusiveScores(min, max, elements);
		return numberOfElementsMeetingCondition;
	}

	public Long zcount(final byte[] key, final byte[] min, final byte[] max) {

		String minValueString = SafeEncoder.encode(min);
		String maxValueString = SafeEncoder.encode(max);

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));

		double minValue = getRealScoreForMinValue(minValueString, elements);
		double maxValue = getRealScoreForMaxValue(maxValueString, elements);

		long numberOfElementsMeetingCondition = countNumberOfElementsBetweenInclusiveScores(minValue, maxValue,
				elements);
		return numberOfElementsMeetingCondition;

	}

	private long countNumberOfElementsBetweenInclusiveScores(final double min, final double max,
			Collection<ScoredByteBuffer> elements) {
		long numberOfElementsMeetingCondition = 0;

		for (ScoredByteBuffer scoredByteBuffer : elements) {
			double score = scoredByteBuffer.getScore();

			if (score >= min && score <= max) {
				numberOfElementsMeetingCondition++;
			}

		}
		return numberOfElementsMeetingCondition;
	}

	/**
	 * If member already exists in the sorted set adds the increment to its
	 * score and updates the position of the element in the sorted set
	 * accordingly. If member does not already exist in the sorted set it is
	 * added with increment as score (that is, like if the previous score was
	 * virtually zero). If key does not exist a new sorted set with the
	 * specified member as sole member is crated. If the key exists but does not
	 * hold a sorted set value an error is returned.
	 * <p>
	 * The score value can be the string representation of a double precision
	 * floating point number. It's possible to provide a negative value to
	 * perform a decrement.
	 * <p>
	 * For an introduction to sorted sets check the Introduction to Redis data
	 * types page.
	 * <p>
	 * Time complexity O(log(N)) with N being the number of elements in the
	 * sorted set
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return The new score
	 */
	public Double zincrby(final byte[] key, final double score, final byte[] member) {

		ByteBuffer wrappedKey = wrap(key);
		ScoredByteBuffer memberToUpdate = findScoredByteBufferByKeyAndMember(member, wrappedKey);

		if (memberToUpdate != null) {
			double newScore = memberToUpdate.getScore() + score;
			removeAndUpdateElement(newScore, member, wrappedKey, memberToUpdate);
			return newScore;
		} else {
			sortset.put(wrappedKey, ScoredByteBuffer.createScoredByteBuffer(wrap(member), score));
			return score;
		}

	}

	/**
	 * Creates a union or intersection of N sorted sets given by keys k1 through
	 * kN, and stores it at dstkey. It is mandatory to provide the number of
	 * input keys N, before passing the input keys and the other (optional)
	 * arguments.
	 * <p>
	 * As the terms imply, the {@link #zinterstore(String, String...)
	 * ZINTERSTORE} command requires an element to be present in each of the
	 * given inputs to be inserted in the result. The
	 * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all
	 * elements across all inputs.
	 * <p>
	 * Using the WEIGHTS option, it is possible to add weight to each input
	 * sorted set. This means that the score of each element in the sorted set
	 * is first multiplied by this weight before being passed to the
	 * aggregation. When this option is not given, all weights default to 1.
	 * <p>
	 * With the AGGREGATE option, it's possible to specify how the results of
	 * the union or intersection are aggregated. This option defaults to SUM,
	 * where the score of an element is summed across the inputs where it
	 * exists. When this option is set to be either MIN or MAX, the resulting
	 * set will contain the minimum or maximum score of an element across the
	 * inputs where it exists.
	 * <p>
	 * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the
	 * sizes of the input sorted sets, and M being the number of elements in the
	 * resulting sorted set
	 * 
	 * @see #zunionstore(String, String...)
	 * @see #zunionstore(String, ZParams, String...)
	 * @see #zinterstore(String, String...)
	 * @see #zinterstore(String, ZParams, String...)
	 * 
	 * @param dstkey
	 * @param sets
	 * @return Integer reply, specifically the number of elements in the sorted
	 *         set at dstkey
	 */
	public Long zunionstore(final byte[] dstkey, final byte[]... sets) {
		ZParams zParams = new ZParams();
		return zunionstore(dstkey, zParams, sets);
	}

	/**
	 * Creates a union or intersection of N sorted sets given by keys k1 through
	 * kN, and stores it at dstkey. It is mandatory to provide the number of
	 * input keys N, before passing the input keys and the other (optional)
	 * arguments.
	 * <p>
	 * As the terms imply, the {@link #zinterstore(String, String...)
	 * ZINTERSTORE} command requires an element to be present in each of the
	 * given inputs to be inserted in the result. The
	 * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all
	 * elements across all inputs.
	 * <p>
	 * Using the WEIGHTS option, it is possible to add weight to each input
	 * sorted set. This means that the score of each element in the sorted set
	 * is first multiplied by this weight before being passed to the
	 * aggregation. When this option is not given, all weights default to 1.
	 * <p>
	 * With the AGGREGATE option, it's possible to specify how the results of
	 * the union or intersection are aggregated. This option defaults to SUM,
	 * where the score of an element is summed across the inputs where it
	 * exists. When this option is set to be either MIN or MAX, the resulting
	 * set will contain the minimum or maximum score of an element across the
	 * inputs where it exists.
	 * <p>
	 * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the
	 * sizes of the input sorted sets, and M being the number of elements in the
	 * resulting sorted set
	 * 
	 * @see #zunionstore(String, String...)
	 * @see #zunionstore(String, ZParams, String...)
	 * @see #zinterstore(String, String...)
	 * @see #zinterstore(String, ZParams, String...)
	 * 
	 * @param dstkey
	 * @param sets
	 * @param params
	 * @return Integer reply, specifically the number of elements in the sorted
	 *         set at dstkey
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Long zunionstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {

		Closure1<List> union = closure(List.class);
		{
			of(this).unionElements(var(List.class));
		}
		return zXStore(dstkey, params, union, sets);

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Long zXStore(final byte[] dstkey, final ZParams params, Closure1<List> operation, final byte[]... sets) {
		sortset.removeAll(wrap(dstkey));

		List<ByteBuffer> parameters = convert(params.getParams(), new ByteArray2ByteBufferConverter());

		String typeOfAggregation = getTypeOfAggregationAndRemoveFromAggregationParameters(parameters);
		List<ByteBuffer> weightValues = getWeightValues(parameters);

		if (areWeightValuesCorrectlySet(weightValues, sets)) {
			Set<ByteBuffer> storeElements = (Set<ByteBuffer>) operation.apply(Arrays.asList(sets));
			updateDestinationWithZParams(dstkey, typeOfAggregation, weightValues, storeElements, sets);

		} else {
			throw new IllegalArgumentException("ERR syntax error. Number of sets and weights are not correct.");
		}

		return zcard(dstkey);
	}

	/**
	 * Creates a union or intersection of N sorted sets given by keys k1 through
	 * kN, and stores it at dstkey. It is mandatory to provide the number of
	 * input keys N, before passing the input keys and the other (optional)
	 * arguments.
	 * <p>
	 * As the terms imply, the {@link #zinterstore(String, String...)
	 * ZINTERSTORE} command requires an element to be present in each of the
	 * given inputs to be inserted in the result. The
	 * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all
	 * elements across all inputs.
	 * <p>
	 * Using the WEIGHTS option, it is possible to add weight to each input
	 * sorted set. This means that the score of each element in the sorted set
	 * is first multiplied by this weight before being passed to the
	 * aggregation. When this option is not given, all weights default to 1.
	 * <p>
	 * With the AGGREGATE option, it's possible to specify how the results of
	 * the union or intersection are aggregated. This option defaults to SUM,
	 * where the score of an element is summed across the inputs where it
	 * exists. When this option is set to be either MIN or MAX, the resulting
	 * set will contain the minimum or maximum score of an element across the
	 * inputs where it exists.
	 * <p>
	 * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the
	 * sizes of the input sorted sets, and M being the number of elements in the
	 * resulting sorted set
	 * 
	 * @see #zunionstore(String, String...)
	 * @see #zunionstore(String, ZParams, String...)
	 * @see #zinterstore(String, String...)
	 * @see #zinterstore(String, ZParams, String...)
	 * 
	 * @param dstkey
	 * @param sets
	 * @return Integer reply, specifically the number of elements in the sorted
	 *         set at dstkey
	 */
	public Long zinterstore(final byte[] dstkey, final byte[]... sets) {

		ZParams zParams = new ZParams();
		return zinterstore(dstkey, zParams, sets);

	}

	/**
	 * Creates a union or intersection of N sorted sets given by keys k1 through
	 * kN, and stores it at dstkey. It is mandatory to provide the number of
	 * input keys N, before passing the input keys and the other (optional)
	 * arguments.
	 * <p>
	 * As the terms imply, the {@link #zinterstore(String, String...)
	 * ZINTERSTORE} command requires an element to be present in each of the
	 * given inputs to be inserted in the result. The
	 * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all
	 * elements across all inputs.
	 * <p>
	 * Using the WEIGHTS option, it is possible to add weight to each input
	 * sorted set. This means that the score of each element in the sorted set
	 * is first multiplied by this weight before being passed to the
	 * aggregation. When this option is not given, all weights default to 1.
	 * <p>
	 * With the AGGREGATE option, it's possible to specify how the results of
	 * the union or intersection are aggregated. This option defaults to SUM,
	 * where the score of an element is summed across the inputs where it
	 * exists. When this option is set to be either MIN or MAX, the resulting
	 * set will contain the minimum or maximum score of an element across the
	 * inputs where it exists.
	 * <p>
	 * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the
	 * sizes of the input sorted sets, and M being the number of elements in the
	 * resulting sorted set
	 * 
	 * @see #zunionstore(String, String...)
	 * @see #zunionstore(String, ZParams, String...)
	 * @see #zinterstore(String, String...)
	 * @see #zinterstore(String, ZParams, String...)
	 * 
	 * @param dstkey
	 * @param sets
	 * @param params
	 * @return Integer reply, specifically the number of elements in the sorted
	 *         set at dstkey
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Long zinterstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {

		Closure1<List> union = closure(List.class);
		{
			of(this).intersactionElements(var(List.class));
		}
		return zXStore(dstkey, params, union, sets);
	}

	public Set<byte[]> zrange(final byte[] key, final long start, final long end) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		Set<ScoredByteBuffer> elementsByRange = getElementsByRange(elements, (int)start, (int)end);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<ScoredByteBuffer> zrangeWithScores(final byte[] key, final long start, final long end) {
		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		return getElementsByRange(elements, (int)start, (int)end);
	}

	/**
	 * Return the all the elements in the sorted set at key with a score between
	 * min and max (including elements with score equal to min or max).
	 * <p>
	 * The elements having the same score are returned sorted lexicographically
	 * as ASCII strings (this follows from a property of Redis sorted sets and
	 * does not involve further computation).
	 * <p>
	 * Using the optional
	 * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
	 * possible to get only a range of the matching elements in an SQL-alike
	 * way. Note that if offset is large the commands needs to traverse the list
	 * for offset elements and this adds up to the O(M) figure.
	 * <p>
	 * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
	 * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
	 * of returning the actual elements in the specified interval, it just
	 * returns the number of matching elements.
	 * <p>
	 * <b>Exclusive intervals and infinity</b>
	 * <p>
	 * min and max can be -inf and +inf, so that you are not required to know
	 * what's the greatest or smallest element in order to take, for instance,
	 * elements "up to a given value".
	 * <p>
	 * Also while the interval is for default closed (inclusive) it's possible
	 * to specify open intervals prefixing the score with a "(" character, so
	 * for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (1.3 5}
	 * <p>
	 * Will return all the values with score > 1.3 and <= 5, while for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (5 (10}
	 * <p>
	 * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * O(log(N))+O(M) with N being the number of elements in the sorted set and
	 * M the number of elements returned by the command, so if M is constant
	 * (for instance you always ask for the first ten elements with LIMIT) you
	 * can consider it O(log(N))
	 * 
	 * @see #zrangeByScore(byte[], double, double)
	 * @see #zrangeByScore(byte[], double, double, int, int)
	 * @see #zrangeByScoreWithScores(byte[], double, double)
	 * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
	 * @see #zcount(byte[], double, double)
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return Multi bulk reply specifically a list of elements in the specified
	 *         score range.
	 */
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), min, max);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));

	}

	private List<ScoredByteBuffer> getElementsByMetaIntervals(final byte[] key, final byte[] min, final byte[] max) {
		String minString = SafeEncoder.encode(min);
		String maxString = SafeEncoder.encode(max);

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));

		double minValue = getRealScoreForMinValue(minString, elements);
		double maxValue = getRealScoreForMaxValue(maxString, elements);

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(elements, minValue, maxValue);
		return elementsByRange;
	}

	/**
	 * Return the all the elements in the sorted set at key with a score between
	 * min and max (including elements with score equal to min or max).
	 * <p>
	 * The elements having the same score are returned sorted lexicographically
	 * as ASCII strings (this follows from a property of Redis sorted sets and
	 * does not involve further computation).
	 * <p>
	 * Using the optional
	 * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
	 * possible to get only a range of the matching elements in an SQL-alike
	 * way. Note that if offset is large the commands needs to traverse the list
	 * for offset elements and this adds up to the O(M) figure.
	 * <p>
	 * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
	 * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
	 * of returning the actual elements in the specified interval, it just
	 * returns the number of matching elements.
	 * <p>
	 * <b>Exclusive intervals and infinity</b>
	 * <p>
	 * min and max can be -inf and +inf, so that you are not required to know
	 * what's the greatest or smallest element in order to take, for instance,
	 * elements "up to a given value".
	 * <p>
	 * Also while the interval is for default closed (inclusive) it's possible
	 * to specify open intervals prefixing the score with a "(" character, so
	 * for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (1.3 5}
	 * <p>
	 * Will return all the values with score > 1.3 and <= 5, while for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (5 (10}
	 * <p>
	 * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * O(log(N))+O(M) with N being the number of elements in the sorted set and
	 * M the number of elements returned by the command, so if M is constant
	 * (for instance you always ask for the first ten elements with LIMIT) you
	 * can consider it O(log(N))
	 * 
	 * @see #zrangeByScore(byte[], double, double)
	 * @see #zrangeByScore(byte[], double, double, int, int)
	 * @see #zrangeByScoreWithScores(byte[], double, double)
	 * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
	 * @see #zcount(byte[], double, double)
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return Multi bulk reply specifically a list of elements in the specified
	 *         score range.
	 */
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
			final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), min, max);
		elementsByRange = limitResult(elementsByRange, offset, count);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
			final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);
		elementsByRange = limitResult(elementsByRange, offset, count);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));

	}

	/**
	 * Return the all the elements in the sorted set at key with a score between
	 * min and max (including elements with score equal to min or max).
	 * <p>
	 * The elements having the same score are returned sorted lexicographically
	 * as ASCII strings (this follows from a property of Redis sorted sets and
	 * does not involve further computation).
	 * <p>
	 * Using the optional
	 * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
	 * possible to get only a range of the matching elements in an SQL-alike
	 * way. Note that if offset is large the commands needs to traverse the list
	 * for offset elements and this adds up to the O(M) figure.
	 * <p>
	 * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
	 * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
	 * of returning the actual elements in the specified interval, it just
	 * returns the number of matching elements.
	 * <p>
	 * <b>Exclusive intervals and infinity</b>
	 * <p>
	 * min and max can be -inf and +inf, so that you are not required to know
	 * what's the greatest or smallest element in order to take, for instance,
	 * elements "up to a given value".
	 * <p>
	 * Also while the interval is for default closed (inclusive) it's possible
	 * to specify open intervals prefixing the score with a "(" character, so
	 * for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (1.3 5}
	 * <p>
	 * Will return all the values with score > 1.3 and <= 5, while for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (5 (10}
	 * <p>
	 * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * O(log(N))+O(M) with N being the number of elements in the sorted set and
	 * M the number of elements returned by the command, so if M is constant
	 * (for instance you always ask for the first ten elements with LIMIT) you
	 * can consider it O(log(N))
	 * 
	 * @see #zrangeByScore(byte[], double, double)
	 * @see #zrangeByScore(byte[], double, double, int, int)
	 * @see #zrangeByScoreWithScores(byte[], double, double)
	 * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
	 * @see #zcount(byte[], double, double)
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return Multi bulk reply specifically a list of elements in the specified
	 *         score range.
	 */
	public Set<ScoredByteBuffer> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
		return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(getElementsByInclusiveRangeScore(
				sortset.get(wrap(key)), min, max));
	}

	public Set<ScoredByteBuffer> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
		return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(getElementsByMetaIntervals(key, min, max));
	}

	/**
	 * Return the all the elements in the sorted set at key with a score between
	 * min and max (including elements with score equal to min or max).
	 * <p>
	 * The elements having the same score are returned sorted lexicographically
	 * as ASCII strings (this follows from a property of Redis sorted sets and
	 * does not involve further computation).
	 * <p>
	 * Using the optional
	 * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
	 * possible to get only a range of the matching elements in an SQL-alike
	 * way. Note that if offset is large the commands needs to traverse the list
	 * for offset elements and this adds up to the O(M) figure.
	 * <p>
	 * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
	 * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
	 * of returning the actual elements in the specified interval, it just
	 * returns the number of matching elements.
	 * <p>
	 * <b>Exclusive intervals and infinity</b>
	 * <p>
	 * min and max can be -inf and +inf, so that you are not required to know
	 * what's the greatest or smallest element in order to take, for instance,
	 * elements "up to a given value".
	 * <p>
	 * Also while the interval is for default closed (inclusive) it's possible
	 * to specify open intervals prefixing the score with a "(" character, so
	 * for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (1.3 5}
	 * <p>
	 * Will return all the values with score > 1.3 and <= 5, while for instance:
	 * <p>
	 * {@code ZRANGEBYSCORE zset (5 (10}
	 * <p>
	 * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * O(log(N))+O(M) with N being the number of elements in the sorted set and
	 * M the number of elements returned by the command, so if M is constant
	 * (for instance you always ask for the first ten elements with LIMIT) you
	 * can consider it O(log(N))
	 * 
	 * @see #zrangeByScore(byte[], double, double)
	 * @see #zrangeByScore(byte[], double, double, int, int)
	 * @see #zrangeByScoreWithScores(byte[], double, double)
	 * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
	 * @see #zcount(byte[], double, double)
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return Multi bulk reply specifically a list of elements in the specified
	 *         score range.
	 */
	public Set<ScoredByteBuffer> zrangeByScoreWithScores(final byte[] key, final double min, final double max,
			final int offset, final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), min, max);
		return new LinkedHashSet<ScoredByteBuffer>(limitResult(elementsByRange, offset, count));

	}

	public Set<ScoredByteBuffer> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max,
			final int offset, final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);
		return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(
				limitResult(elementsByRange, offset, count));
	}

	private List<ScoredByteBuffer> limitResult(List<ScoredByteBuffer> elements, int offset, int count) {
		return RangeUtils.limitListByOffsetCount(offset, count, elements);
	}

	private List<ScoredByteBuffer> getElementsByInclusiveRangeScore(Collection<ScoredByteBuffer> elements,
			final double min, final double max) {

		List<ScoredByteBuffer> subSelect = filter(
				having(on(ScoredByteBuffer.class).getScore(), greaterThanOrEqualTo(min)).and(
						having(on(ScoredByteBuffer.class).getScore(), lessThanOrEqualTo(max))), elements);

		return new LinkedList<SortsetDatatypeOperations.ScoredByteBuffer>(subSelect);

	}

	/**
	 * Return the rank (or index) or member in the sorted set at key, with
	 * scores being ordered from low to high.
	 * <p>
	 * When the given member does not exist in the sorted set, the special value
	 * 'nil' is returned. The returned rank (or index) of the member is 0-based
	 * for both commands.
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * O(log(N))
	 * 
	 * @see #zrevrank(byte[], byte[])
	 * 
	 * @param key
	 * @param member
	 * @return Integer reply or a nil bulk reply, specifically: the rank of the
	 *         element as an integer reply if the element exists. A nil bulk
	 *         reply if there is no such element.
	 */
	public Long zrank(final byte[] key, final byte[] member) {

		Collection<ScoredByteBuffer> elements = this.sortset.get(wrap(key));

		long position = 0;

		for (ScoredByteBuffer scoredByteBuffer : elements) {

			if (scoredByteBuffer.byteBuffer.equals(wrap(member))) {
				return position;
			}

			position++;
		}

		return null;
	}

	/**
	 * Remove the specified member from the sorted set value stored at key. If
	 * member was not a member of the set no operation is performed. If key does
	 * not not hold a set value an error is returned.
	 * <p>
	 * 
	 * @param key
	 * @param members
	 * @return The number of members removed from the sorted set, not including
	 *         non existing members.
	 */
	public Long zrem(final byte[] key, final byte[]... members) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));

		long removedElements = 0;

		for (byte[] member : members) {

			int numberOfElements = elements.size();
			int numberOfNoneRemovedElements = with(elements).remove(
					having(on(ScoredByteBuffer.class).getByteBuffer(), equalTo(wrap(member)))).size();

			if (numberOfElements != numberOfNoneRemovedElements) {
				removedElements++;
			}

		}

		return removedElements;

	}

	/**
	 * Remove all elements in the sorted set at key with rank between start and
	 * end. Start and end are 0-based with rank 0 being the element with the
	 * lowest score. Both start and end can be negative numbers, where they
	 * indicate offsets starting at the element with the highest rank. For
	 * example: -1 is the element with the highest score, -2 the element with
	 * the second highest score and so forth.
	 * <p>
	 * <b>Time complexity:</b> O(log(N))+O(M) with N being the number of
	 * elements in the sorted set and M the number of elements removed by the
	 * operation
	 * 
	 */
	public Long zremrangeByRank(final byte[] key, final long start, final long end) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		Set<ScoredByteBuffer> elementsToRemove = getElementsByRange(elements, (int)start, (int)end);

		Collection<ScoredByteBuffer> allElements = sortset.get(wrap(key));

		with(allElements).removeAll(elementsToRemove);

		return (long) elementsToRemove.size();

	}

	/**
	 * Remove all the elements in the sorted set at key with a score between min
	 * and max (including elements with score equal to min or max).
	 * <p>
	 * <b>Time complexity:</b>
	 * <p>
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return Integer reply, specifically the number of elements removed.
	 */
	public Long zremrangeByScore(final byte[] key, final double start, final double end) {

		List<ScoredByteBuffer> elementsToRemove = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), start, end);
		return removeListOfElements(key, elementsToRemove);

	}

	public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {

		List<ScoredByteBuffer> elementsToRemove = getElementsByMetaIntervals(key, start, end);
		return removeListOfElements(key, elementsToRemove);

	}

	public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elements);
		Set<ScoredByteBuffer> elementsByRange = getElementsByRange(reverseOrderElements, (int)start, (int)end);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));

	}

	public Set<ScoredByteBuffer> zrevrangeWithScores(final byte[] key, final long start, final long end) {
		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elements);
		Set<ScoredByteBuffer> elementsByRange = getElementsByRange(reverseOrderElements, (int)start, (int)end);

		return elementsByRange;
	}

	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(elements, min, max);
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);

		return new LinkedHashSet<byte[]>(convert(
				extract(reverseOrderElements, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);

		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);

		return new LinkedHashSet<byte[]>(convert(
				extract(reverseOrderElements, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
			final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), min, max);
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);
		elementsByRange = limitResult(new LinkedList<ScoredByteBuffer>(reverseOrderElements), offset, count);

		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));

	}

	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);

		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);
		elementsByRange = limitResult(new LinkedList<ScoredByteBuffer>(reverseOrderElements), offset, count);
		return new LinkedHashSet<byte[]>(convert(extract(elementsByRange, on(ScoredByteBuffer.class).getByteBuffer()),
				new ByteBuffer2ByteArrayConverter()));
	}

	public Set<ScoredByteBuffer> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {

		Collection<ScoredByteBuffer> elements = sortset.get(wrap(key));

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(elements, min, max);
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);

		return reverseOrderElements;

	}

	public Set<ScoredByteBuffer> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
			final int offset, final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByInclusiveRangeScore(sortset.get(wrap(key)), min, max);
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);
		return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(limitResult(
				new LinkedList<ScoredByteBuffer>(reverseOrderElements), offset, count));

	}

	public Set<ScoredByteBuffer> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);
		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);

		return reverseOrderElements;

	}

	public Set<ScoredByteBuffer> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
			final int offset, final int count) {

		List<ScoredByteBuffer> elementsByRange = getElementsByMetaIntervals(key, min, max);

		Set<ScoredByteBuffer> reverseOrderElements = reverseElements(elementsByRange);
		return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(limitResult(
				new LinkedList<ScoredByteBuffer>(reverseOrderElements), offset, count));

	}
	
	/**
     * Return the rank (or index) or member in the sorted set at key, with
     * scores being ordered from high to low.
     * <p>
     * When the given member does not exist in the sorted set, the special value
     * 'nil' is returned. The returned rank (or index) of the member is 0-based
     * for both commands.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * 
     * @see #zrank(byte[], byte[])
     * 
     * @param key
     * @param member
     * @return Integer reply or a nil bulk reply, specifically: the rank of the
     *         element as an integer reply if the element exists. A nil bulk
     *         reply if there is no such element.
     */
    public Long zrevrank(final byte[] key, final byte[] member) {
    	
    	Collection<ScoredByteBuffer> elements = this.sortset.get(wrap(key));
    	Set<ScoredByteBuffer> reverseElements = reverseElements(elements);
    	
		long position = 0;

		for (ScoredByteBuffer scoredByteBuffer : reverseElements) {

			if (scoredByteBuffer.byteBuffer.equals(wrap(member))) {
				return position;
			}

			position++;
		}

		return null;
    	
    }

    /**
     * Return the score of the specified element of the sorted set at key. If
     * the specified element does not exist in the sorted set, or the key does
     * not exist at all, a special 'nil' value is returned.
     * <p>
     * 
     * @param key
     * @param member
     * @return the score
     */
    public Double zscore(final byte[] key, final byte[] member) {
    	
    	Collection<ScoredByteBuffer> elements = this.sortset.get(wrap(key));
    	
    	ScoredByteBuffer requiredElement = selectUnique(elements, having(on(ScoredByteBuffer.class).getByteBuffer(), equalTo(wrap(member))));
    	
    	if(requiredElement != null) {
    		return requiredElement.getScore();
    	}
    	
    	return null;
    	
    }
    
	private Set<ScoredByteBuffer> reverseElements(Collection<ScoredByteBuffer> elementsByRange) {
		Comparator<ScoredByteBuffer> reverseOrder = Collections.reverseOrder();
		Set<ScoredByteBuffer> reverseOrderElements = new TreeSet<SortsetDatatypeOperations.ScoredByteBuffer>(
				reverseOrder);
		reverseOrderElements.addAll(elementsByRange);
		return reverseOrderElements;
	}

	private Long removeListOfElements(final byte[] key, List<ScoredByteBuffer> elementsToRemove) {
		Collection<ScoredByteBuffer> allElements = sortset.get(wrap(key));
		with(allElements).removeAll(elementsToRemove);

		return (long) elementsToRemove.size();
	}

	@SuppressWarnings("unchecked")
	private Set<ScoredByteBuffer> getElementsByRange(Collection<ScoredByteBuffer> elements, final int start,
			final int end) {

		List<ScoredByteBuffer> listOfElements = new LinkedList<ScoredByteBuffer>(elements);

		int calculatedStart = RangeUtils.calculateStart(start, listOfElements.size());
		int calculatedEnd = RangeUtils.calculateEnd(end, listOfElements.size());

		try {
			List<ScoredByteBuffer> subList = listOfElements.subList(calculatedStart, calculatedEnd);
			return new LinkedHashSet<SortsetDatatypeOperations.ScoredByteBuffer>(subList);
		} catch (ArrayIndexOutOfBoundsException e) {
			return Collections.EMPTY_SET;
		}

	}

	public long getNumberOfKeys() {
		return this.sortset.keySet().size();
	}
	
	public void flushAllKeys() {
		removeExpirations();
		this.sortset.clear();
	}

	private void removeExpirations() {
		List<byte[]> keys = this.keys();
		for (byte[] key : keys) {
			this.removeExpiration(key);
		}
	}
	
	private void updateDestinationWithZParams(final byte[] dstkey, String typeOfAggregation,
			List<ByteBuffer> weightValues, Set<ByteBuffer> elements, final byte[]... sets) {
		for (int i = 0; i < sets.length; i++) {
			byte[] setKey = sets[i];
			ByteBuffer wrappedKey = wrap(setKey);
			for (ByteBuffer elementBuffer : elements) {
				ScoredByteBuffer element = findScoredByteBufferByKeyAndMember(elementBuffer.array(), wrappedKey);

				if (element != null) {
					double newScore = element.getScore() * multiplicationFactor(weightValues, i);
					if (ZParams.Aggregate.SUM.name().equals(typeOfAggregation)) {
						zincrby(dstkey, newScore, elementBuffer.array());
					} else {
						if (ZParams.Aggregate.MIN.name().equals(typeOfAggregation)) {
							zincrmin(dstkey, newScore, elementBuffer.array());
						} else {
							if (ZParams.Aggregate.MAX.name().equals(typeOfAggregation)) {
								zincrmax(dstkey, newScore, elementBuffer.array());
							}
						}
					}
				}
			}
		}
	}

	private Double zincrmax(final byte[] key, final double score, final byte[] member) {

		ByteBuffer wrappedKey = wrap(key);
		ScoredByteBuffer memberToUpdate = findScoredByteBufferByKeyAndMember(member, wrappedKey);

		if (memberToUpdate != null) {
			double newScore = memberToUpdate.getScore() < score ? score : memberToUpdate.getScore();
			removeAndUpdateElement(newScore, member, wrappedKey, memberToUpdate);
			return newScore;
		} else {
			sortset.put(wrappedKey, ScoredByteBuffer.createScoredByteBuffer(wrap(member), score));
			return score;
		}

	}

	private Double zincrmin(final byte[] key, final double score, final byte[] member) {

		ByteBuffer wrappedKey = wrap(key);
		ScoredByteBuffer memberToUpdate = findScoredByteBufferByKeyAndMember(member, wrappedKey);

		if (memberToUpdate != null) {
			double newScore = memberToUpdate.getScore() > score ? score : memberToUpdate.getScore();
			removeAndUpdateElement(newScore, member, wrappedKey, memberToUpdate);
			return newScore;
		} else {
			sortset.put(wrappedKey, ScoredByteBuffer.createScoredByteBuffer(wrap(member), score));
			return score;
		}

	}

	private int multiplicationFactor(List<ByteBuffer> weightValues, int index) {

		if (weightValues.size() == 0) {
			return 1;
		} else {
			return Integer.parseInt(SafeEncoder.encode(weightValues.get(index).array()));
		}
	}

	private boolean areWeightValuesCorrectlySet(List<ByteBuffer> weightValues, final byte[]... sets) {
		return weightValues.size() == 0 || weightValues.size() == sets.length;
	}

	@SuppressWarnings("unchecked")
	private List<ByteBuffer> getWeightValues(List<ByteBuffer> parameters) {
		if (parameters.contains(wrap(WEIGHTS.raw))) {
			int weightsWordIndex = parameters.indexOf(wrap(WEIGHTS.raw));
			parameters.remove(weightsWordIndex);
			return parameters;
		} else {
			return Collections.EMPTY_LIST;
		}
	}

	private String getTypeOfAggregationAndRemoveFromAggregationParameters(List<ByteBuffer> parameters) {
		String typeOfAggregation = ZParams.Aggregate.SUM.name();
		if (parameters.contains(wrap(AGGREGATE.raw))) {
			int aggregateWordIndex = parameters.indexOf(wrap(AGGREGATE.raw));
			typeOfAggregation = SafeEncoder.encode((parameters.get(aggregateWordIndex + 1).array()));
			parameters.remove(aggregateWordIndex);
			parameters.remove(aggregateWordIndex);
		}
		return typeOfAggregation;
	}

	protected Set<ByteBuffer> unionElements(final List<byte[]> keys) {
		Set<ScoredByteBuffer> unionElements = new HashSet<ScoredByteBuffer>();

		for (byte[] key : keys) {
			unionElements.addAll(sortset.get(wrap(key)));
		}

		return new HashSet<ByteBuffer>(extract(unionElements, on(ScoredByteBuffer.class).getByteBuffer()));
	}

	protected Set<ByteBuffer> intersactionElements(final List<byte[]> keys) {

		if (keys.size() == 0) {
			return new HashSet<ByteBuffer>();
		}

		Set<ByteBuffer> targetKey = new HashSet<ByteBuffer>(getReferenceElement(keys));
		targetKey = retainElements(targetKey, keys);
		return targetKey;
	}

	private Set<ByteBuffer> retainElements(Set<ByteBuffer> targetKey, final List<byte[]> keys) {
		for (int index = 1; index < keys.size(); index++) {
			Collection<ScoredByteBuffer> collectionElements = sortset.get(wrap(keys.get(index)));

			List<ByteBuffer> extract = extract(collectionElements, on(ScoredByteBuffer.class).getByteBuffer());
			targetKey.retainAll(extract);
		}

		return targetKey;
	}

	private List<ByteBuffer> getReferenceElement(final List<byte[]> keys) {
		Collection<ScoredByteBuffer> referenceElements = sortset.get(wrap(keys.get(0)));
		return extract(referenceElements, on(ScoredByteBuffer.class).getByteBuffer());
	}

	protected static class ScoredByteBuffer implements Comparable<ScoredByteBuffer> {

		private double score;
		private ByteBuffer byteBuffer;

		private ScoredByteBuffer(ByteBuffer byteBuffer, double score) {
			this.score = score;
			this.byteBuffer = byteBuffer;
		}

		public static ScoredByteBuffer createScoredByteBuffer(ByteBuffer byteBuffer, double score) {
			return new ScoredByteBuffer(byteBuffer, score);
		}

		public double getScore() {
			return score;
		}

		public ByteBuffer getByteBuffer() {
			return byteBuffer;
		}

		@Override
		public int compareTo(ScoredByteBuffer scoredByteBuffer) {
			if (this.score == scoredByteBuffer.getScore()) {
				return this.byteBuffer.compareTo(scoredByteBuffer.getByteBuffer());
			} else {
				if (this.score > scoredByteBuffer.getScore()) {
					return 1;
				} else {
					return -1;
				}
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((byteBuffer == null) ? 0 : byteBuffer.hashCode());
			long temp;
			temp = Double.doubleToLongBits(score);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ScoredByteBuffer other = (ScoredByteBuffer) obj;
			if (byteBuffer == null) {
				if (other.byteBuffer != null)
					return false;
			} else if (!byteBuffer.equals(other.byteBuffer))
				return false;
			if (Double.doubleToLongBits(score) != Double.doubleToLongBits(other.score))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return new String(byteBuffer.array()) + " - " + score;
		}

	}

	@Override
	public Long del(byte[]... keys) {
		long numberOfRemovedElements = 0;
		
		for (byte[] key : keys) {
			ByteBuffer wrappedKey = wrap(key);
			if(this.sortset.containsKey(wrappedKey)) {
				this.sortset.removeAll(wrappedKey);
				removeExpiration(key);
				numberOfRemovedElements++;
			}
		}
		
		return numberOfRemovedElements;
	}

	@Override
	public boolean exists(byte[] key) {
		return this.sortset.containsKey(wrap(key));
	}

	@Override
	public boolean renameKey(byte[] key, byte[] newKey) {
		ByteBuffer wrappedKey = wrap(key);

		if (this.sortset.containsKey(wrappedKey)) {
			Collection<ScoredByteBuffer> elements = this.sortset.get(wrappedKey);
			this.sortset.removeAll(wrap(newKey));
			this.sortset.putAll(wrap(newKey), elements);
			this.sortset.removeAll(wrappedKey);
			
			renameTtlKey(key, newKey);
			
			return true;
		}

		return false;
	}

	@Override
	public List<byte[]> keys() {
		return new ArrayList<byte[]>(convert(this.sortset.keySet(),
				ByteBuffer2ByteArrayConverter.createByteBufferConverter()));
	}

	@Override
	public String type() {
		return ZSET;
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		try {
			return sortNumberValues(key);
		} catch (NumberFormatException e) {
			Collection<ScoredByteBuffer> scoredElements = this.sortset.get(wrap(key));
			Collection<ByteBuffer> elements = convert(scoredElements, ScoredByteBufferToByteBuffer.createScoredByteBufferToByteBufferConverter());
			return convert(elements,
					ByteBuffer2ByteArrayConverter.createByteBufferConverter());
		}
	}

	private List<byte[]> sortNumberValues(byte[] key) {
		Collection<ScoredByteBuffer> scoredElements = this.sortset.get(wrap(key));
		Collection<ByteBuffer> elements = convert(scoredElements, ScoredByteBufferToByteBuffer.createScoredByteBufferToByteBufferConverter());
		
		List<Double> values = convert(elements, ByteBufferAsString2DoubleConverter.createByteBufferAsStringToDoubleConverter());
		
		Collections.sort(values);
		return new LinkedList<byte[]>(convert(values,
				DoubleToStringByteArrayConverter.createDoubleToStringByteArrayConverter()));
	}
	
	private static class ScoredByteBufferToByteBuffer implements Converter<ScoredByteBuffer, ByteBuffer> {

		private static ScoredByteBufferToByteBuffer createScoredByteBufferToByteBufferConverter() {
			return new ScoredByteBufferToByteBuffer();
		}
		
		@Override
		public ByteBuffer convert(ScoredByteBuffer from) {
			return from.getByteBuffer();
		}
		
	}
	
}
