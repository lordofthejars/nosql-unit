package com.lordofthejars.nosqlunit.util;

import static ch.lambdaj.collection.LambdaCollections.with;
import static org.hamcrest.CoreMatchers.anything;

import java.util.HashMap;
import java.util.Map;


public class EmbeddedInstances<T> {
	
	private Map<String, ThreadLocal<T>> instances = new HashMap<String, ThreadLocal<T>>();
	
	public EmbeddedInstances() {
		super();
	}
	
	public void addEmbeddedInstance(T embeddedInstance, String targetPath) {
		
		if(!instances.containsKey(targetPath)) {
			this.instances.put(targetPath, new ThreadLocal<T>());
		}
		
		ThreadLocal<T> currentThreadLocal = this.instances.get(targetPath);
		currentThreadLocal.set(embeddedInstance);
		
		this.instances.put(targetPath, currentThreadLocal);
	}
	
	public void removeEmbeddedInstance(String targetPath) {
		
		if(instances.containsKey(targetPath)) {
			this.instances.get(targetPath).remove();
		}
	}
	
	public T getEmbeddedByTargetPath(String targetPath) {
		
		if(this.instances.containsKey(targetPath)) {
			return this.instances.get(targetPath).get();
		}

		return null;
		
	}
	
	public T getDefaultEmbeddedInstance() {
		ThreadLocal<T>  currentThreadLocal = with(this.instances).values().first(anything());
		
		if(currentThreadLocal != null) {
			return currentThreadLocal.get();
		}
		
		return null;
	}
	
}
