package com.lordofthejars.nosqlunit.util;

import static ch.lambdaj.collection.LambdaCollections.with;
import static org.hamcrest.CoreMatchers.anything;

import java.util.HashMap;
import java.util.Map;


public class EmbeddedInstances<T> {
	
	private Map<String, T> instances = new HashMap<String, T>();
	
	public EmbeddedInstances() {
		super();
	}
	
	public void addEmbeddedInstance(T embeddedInstance, String targetPath) {
		this.instances.put(targetPath, embeddedInstance);
	}
	
	public void removeEmbeddedInstance(String targetPath) {
		this.instances.remove(targetPath);
	}
	
	public T getEmbeddedByTargetPath(String targetPath) {
		
		if(this.instances.containsKey(targetPath)) {
			return this.instances.get(targetPath);
		}

		return null;
		
	}
	
	public T getDefaultEmbeddedInstance() {
		T  element = with(this.instances).values().first(anything());
		return element;
	}
	
}
