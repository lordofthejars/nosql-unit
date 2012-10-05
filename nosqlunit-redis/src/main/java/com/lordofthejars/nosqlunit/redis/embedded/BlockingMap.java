package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.Collection;
import java.util.Set;


public interface BlockingMap<K, V> {

	V getAndWait(K key) throws InterruptedException;
	V getAndWait(K key, long timeout) throws InterruptedException;
	V lastAndWait(K key) throws InterruptedException;
	V lastAndWait(K key, long timeout) throws InterruptedException;
	V put(K key, V value);
	void putLast(K key, Collection<V> elements);
	void putFirst(K key, Collection<V> elements);
	V pollFirst(K key);
	V pollLast(K key);
	V getElement(K key, int index);
	int size(K key);
	V addElementAt(K key, V value, int index);
	int indexOf(K key, V value);
	Collection<V> elements(K key);
	boolean containsKey(K key);
	V remove(K key, int index);
	int lastIndexOf(K key, V value);
	void clear(K key);
	void replaceValues(K key, Collection<V> newElements);
	int size();
	void clear();
	Set<K> keySet();
}
