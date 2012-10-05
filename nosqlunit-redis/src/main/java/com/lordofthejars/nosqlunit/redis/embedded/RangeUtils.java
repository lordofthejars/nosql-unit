package com.lordofthejars.nosqlunit.redis.embedded;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.select;
import static ch.lambdaj.Lambda.selectFirst;
import static ch.lambdaj.Lambda.selectMax;
import static ch.lambdaj.Lambda.selectMin;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.lordofthejars.nosqlunit.redis.embedded.SortsetDatatypeOperations.ScoredByteBuffer;

public class RangeUtils {

	private static final String NEGATIVE_INFINITE = "-inf";
	private static final String POSITIVE_INFINITE = "+inf";
	private static final String EXCLUSIVE_SYMBOL = "(";

	private RangeUtils() {
		super();
	}
	
	public static byte[] concat(byte[] first, byte[] second) {
		  byte[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
	}
	
	public static int calculateEnd(int end, int size) {
		if (end >= size) {
			return size;
		} else {
			if (end < 0) {
				end++;
				return size + end;
			} else {
				return end + 1;
			}
		}
	}

	public static long calculateStart(long start, long size) {
		return start < 0 ? size + start : start;
	}
	
	public static int calculateStart(int start, int size) {
		return start < 0 ? size + start : start;
	}
	
	@SuppressWarnings("unchecked")
	public static List<ScoredByteBuffer> limitListByOffsetCount(int offset, int count, List<ScoredByteBuffer> elements) {
		if(offset >= elements.size()) {
			return Collections.EMPTY_LIST;
		} else {
			return elements.subList(offset, endIndexByCount(count, offset, elements.size()));
		}
	}
	
	private static int endIndexByCount(int count, int offset, int size) {
		
		if(count < 0) {
			return size;
		}
		
		if(offset+count > size) {
			return size;
		}
		
		return offset+count;
		
	}
	
	public static double getRealScoreForMinValue(String value, Collection<ScoredByteBuffer> elements) {

		if (NEGATIVE_INFINITE.equals(value)) {
			return findMinElement(elements);
		} else {
			if (POSITIVE_INFINITE.equals(value)) {
				return findMaxElement(elements);
			} else {
				if (value.startsWith(EXCLUSIVE_SYMBOL)) {
					return findMinExclusiveValue(value, elements);
				} else {
					return Double.parseDouble(value);
				}
			}
		}
	}

	private static double findMinExclusiveValue(String value, Collection<ScoredByteBuffer> elements) {
		double score = Double.parseDouble(value.substring(1, value.length()));
		ScoredByteBuffer selectFirst = (ScoredByteBuffer)selectFirst(elements, having(on(ScoredByteBuffer.class).getScore(), greaterThan(score)));
		return selectFirst == null ? Double.POSITIVE_INFINITY : selectFirst.getScore();
	}
	
	public static double getRealScoreForMaxValue(String value, Collection<ScoredByteBuffer> elements) {

		if (NEGATIVE_INFINITE.equals(value)) {
			return findMinElement(elements);
		} else {
			if (POSITIVE_INFINITE.equals(value)) {
				return findMaxElement(elements);
			} else {
				if (value.startsWith(EXCLUSIVE_SYMBOL)) {
					return findMaxExclusiveValue(value, elements);
				} else {
					return Double.parseDouble(value);
				}
			}
		}
	}

	private static double findMaxExclusiveValue(String value, Collection<ScoredByteBuffer> elements) {
		double score = Double.parseDouble(value.substring(1, value.length()));
		List<ScoredByteBuffer> elementsWithLessScore = select(elements, having(on(ScoredByteBuffer.class).getScore(), lessThan(score)));
		return elementsWithLessScore.size() == 0 ? Double.NEGATIVE_INFINITY :elementsWithLessScore.get(elementsWithLessScore.size()-1).getScore();
	}
	
	private static double findMinElement(Collection<ScoredByteBuffer> elements) {
		ScoredByteBuffer selectMin = (ScoredByteBuffer)selectMin(elements, on(ScoredByteBuffer.class).getScore());
		return selectMin == null ? 0L : selectMin.getScore();
	}

	private static double findMaxElement(Collection<ScoredByteBuffer> elements) {
		ScoredByteBuffer selectMax = (ScoredByteBuffer)selectMax(elements, on(ScoredByteBuffer.class).getScore());
		return selectMax == null ? 0L : selectMax.getScore();
	}
	
}
