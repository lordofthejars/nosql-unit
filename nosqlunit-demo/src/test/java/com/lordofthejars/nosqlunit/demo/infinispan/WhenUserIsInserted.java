package com.lordofthejars.nosqlunit.demo.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.InfinispanRule.InfinispanRuleBuilder.newInfinispanRule;
import static com.lordofthejars.nosqlunit.infinispan.ManagedInfinispan.ManagedInfinispanRuleBuilder.newManagedInfinispanRule;

import javax.inject.Inject;

import org.infinispan.api.BasicCache;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.infinispan.InfinispanRule;
import com.lordofthejars.nosqlunit.infinispan.ManagedInfinispan;


public class WhenUserIsInserted {

	@ClassRule
	public static final ManagedInfinispan MANAGED_INFINISPAN = newManagedInfinispanRule().infinispanPath("/opt/infinispan-5.1.6").build();
	
	@Rule
	public final InfinispanRule infinispanRule = newInfinispanRule().defaultManagedInfinispan();
	
	@Inject
	private BasicCache<String, User> remoteCache;
	
	@UsingDataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	@ShouldMatchDataSet(location="user.json")
	@Test
	public void user_should_be_available_in_cache() {
		
		UserManager userManager = new UserManager(remoteCache);
		userManager.addUser(new User("alex", 32));
	}
	
}
