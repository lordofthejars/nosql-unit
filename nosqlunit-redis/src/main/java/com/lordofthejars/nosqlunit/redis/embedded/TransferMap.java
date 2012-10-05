package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TransferMap<K, V> implements BlockingMap<K, V> {

	private volatile Map<K, LinkedBlockingDeque<V>> backingMap = new HashMap<K, LinkedBlockingDeque<V>>();

	public static <K, V> TransferMap<K, V> create() {
		return new TransferMap<K, V>();
	}
	
	@Override
	public V getAndWait(K key, long timeout) {

		LinkedBlockingDeque<V> syncrhonousQueue = getNewQueueOrQueueByKey(key);
		try {
			V pollElement = syncrhonousQueue.pollFirst(timeout, TimeUnit.SECONDS);

			return pollElement;
		} catch (InterruptedException e) {
			return null;
		}

	}

	@Override
	public V getAndWait(K key) {
		LinkedBlockingDeque<V> syncrhonousQueue = getNewQueueOrQueueByKey(key);
		try {
			V takeElement = syncrhonousQueue.takeFirst();

			return takeElement;

		} catch (InterruptedException e) {
			return null;
		}
	}

	private LinkedBlockingDeque<V> getNewQueueOrQueueByKey(K key) {

		synchronized (backingMap) {
			if (!backingMap.containsKey(key)) {
				backingMap.put(key, new LinkedBlockingDeque<V>());
			}
			
			return backingMap.get(key);			
		}
	}


	@Override
	public V put(K key, V value) {
		LinkedBlockingDeque<V> synchronousQueue = getNewQueueOrQueueByKey(key);
		synchronousQueue.add(value);
		backingMap.put(key, synchronousQueue);
		return value;
	}

	@Override
	public V lastAndWait(K key) throws InterruptedException {
		LinkedBlockingDeque<V> syncrhonousQueue = getNewQueueOrQueueByKey(key);
		try {
			V takeElement = syncrhonousQueue.takeLast();

			return takeElement;

		} catch (InterruptedException e) {
			return null;
		}
	}

	@Override
	public V lastAndWait(K key, long timeout) throws InterruptedException {

		LinkedBlockingDeque<V> syncrhonousQueue = getNewQueueOrQueueByKey(key);
		try {
			V pollElement = syncrhonousQueue.pollLast(timeout, TimeUnit.SECONDS);

			return pollElement;
		} catch (InterruptedException e) {
			return null;
		}
	}

	@Override
	public V getElement(K key, int index) {
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(index < 0 && elements != null) {
			index = elements.size() + index;
		}
		
		return elementAt(index, elements);
	}

	@Override
	public int size() {
		return backingMap.size();
	}
	
	@Override
	public void clear() {
		backingMap.clear();
	}
	
	@Override
	public int size(K key) {
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(elements != null) {
			return elements.size();
		}
		
		return 0;
	}

	@Override
	public V addElementAt(K key, V value, int index) {
		
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(elements != null) {
			
			List<V> newElements = new ArrayList<V>();
			elements.drainTo(newElements);
			newElements.add(index, value);
			
			elements.addAll(newElements);
			backingMap.put(key, elements);
			
			return value;
		}
		
		return null;
	}

	
	@Override
	public V pollFirst(K key) {
		
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(elements != null) {
			return elements.pollFirst();
		}
		
		return null;
	}

	@Override
	public V pollLast(K key) {
		
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(elements != null) {
			return elements.pollLast();
		}
		
		return null;
	}

	
	@Override
	public void putLast(K key, Collection<V> newElements) {
		
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			backingMap.put(key, insertElementsAtLast(newElements, elements));
		} else {
			LinkedBlockingDeque<V> elements = new LinkedBlockingDeque<V>();
			backingMap.put(key, insertElementsAtLast(newElements, elements));
		}
	}

	private LinkedBlockingDeque<V> insertElementsAtLast(Collection<V> newElements, LinkedBlockingDeque<V> elements) {
		for (V v : newElements) {
			elements.addLast(v);
		}
		
		return elements;
	}
	
	
	
	@Override
	public Set<K> keySet() {
		return this.backingMap.keySet();
	}

	@Override
	public void clear(K key) {
		if(backingMap.containsKey(key)) {
			backingMap.remove(key);
		}
	}

	@Override
	public V remove(K key, int index) {
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			removeElementAtIndex(index, elements);
			backingMap.put(key, elements);
			V element = elementAt(index, elements);
			return element;
		}
		return null;
	}

	private void removeElementAtIndex(int index, LinkedBlockingDeque<V> elements) {
		Iterator<V> iterator = elements.iterator();
		
		int currentIndex = 0;
		
		while(iterator.hasNext()) {
			iterator.next();
			if(currentIndex == index) {
				iterator.remove();
				break;
			}
			currentIndex++;
		}
	}

	@Override
	public void putFirst(K key, Collection<V> newElements) {
		
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			backingMap.put(key, insertElementsAtFirst(newElements, elements));
		} else {
			LinkedBlockingDeque<V> elements = new LinkedBlockingDeque<V>();
			backingMap.put(key, insertElementsAtFirst(newElements, elements));
		}
	}


	private LinkedBlockingDeque<V> insertElementsAtFirst(Collection<V> newElements, LinkedBlockingDeque<V> elements) {
		for (V v : newElements) {
			elements.addFirst(v);
		}
		
		return elements;
	}
	
	@Override
	public boolean containsKey(K key) {
		return backingMap.containsKey(key);
	}

	@Override
	public void replaceValues(K key, Collection<V> newElements) {
		
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			elements.clear();
			elements.addAll(newElements);
		}
		
	}
	
	@Override
	public Collection<V> elements(K key) {
		
		LinkedBlockingDeque<V> elements = backingMap.get(key);
		
		if(elements != null) {
			return elements;
		}
		
		return new LinkedBlockingDeque<V>();
	}

	@Override
	public int indexOf(K key, V value) {
		
		
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			int i=0;
			
			for (V element : elements) {
				if(element.equals(value)) {
					return i;
				}
				i++;
			}
			
		}
		
		return -1;
	}

	@Override
	public int lastIndexOf(K key, V value) {
		
		if(backingMap.containsKey(key)) {
			LinkedBlockingDeque<V> elements = backingMap.get(key);
			int i=elements.size()-1;
			
			Iterator<V> descendingIterator = elements.descendingIterator();
			
			while(descendingIterator.hasNext()) {
				if(descendingIterator.next().equals(value)) {
					return i;
				}
				i--;
			}
		}
		
		return -1;
	}

	private V elementAt(int index, LinkedBlockingDeque<V> elements) {
		if(elements != null && index < elements.size()) {
			int i=0;
			for (V element : elements) {
				
				if(i == index) {
					return element;
				}
				
				i++;
			}
		}
		
		return null;
	}


}
