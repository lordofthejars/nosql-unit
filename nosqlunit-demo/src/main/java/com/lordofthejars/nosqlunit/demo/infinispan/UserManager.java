package com.lordofthejars.nosqlunit.demo.infinispan;

import org.infinispan.api.BasicCache;

public class UserManager {

	private BasicCache<String, User> cache;
	
	public UserManager(BasicCache<String, User> cache) {
		this.cache = cache;
	}
	
	public void addUser(User user) {
		this.cache.put(user.getName(), user);
	}
	
	public User getUser(String name) {
		return this.cache.get(name);
	}
	
}
