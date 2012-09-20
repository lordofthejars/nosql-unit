package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TransferMap<K, V> implements Map<K, V> {
	private final HashMap<K, V> backingMap = new HashMap<K, V>();

	private Map<K,Object> locks = new HashMap<K, Object>();
	
	private final Object lock = new Object();

	public V getAndWait(Object key) throws InterruptedException {
		V value = null;
		synchronized (lock) {
			do {
				value = backingMap.get(key);

				if (value == null)
					lock.wait();

			} while (value == null);
		}
		return value;
	}

	public V put(K key, V value) {
		synchronized (lock) {
			value = backingMap.put(key, value);
			lock.notifyAll();
		}
		return value;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean containsKey(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsValue(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V get(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public V remove(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}
}
