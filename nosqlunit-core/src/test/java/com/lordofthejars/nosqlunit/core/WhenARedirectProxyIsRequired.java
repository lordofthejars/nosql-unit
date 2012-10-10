package com.lordofthejars.nosqlunit.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.lordofthejars.nosqlunit.proxy.RedirectProxy;


public class WhenARedirectProxyIsRequired {

	@Test
	public void redirect_proxy_should_be_created_by_specifing_source_class_and_calling_destination_class() {
		
		ArrayList list = RedirectProxy.createProxy(ArrayList.class, new MyNewList());
		assertThat(list.size(), is(2));
		
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void redirect_proxy_should_throw_an_exception_if_destination_class_does_not_contain_source_method() {
		
		ArrayList list = RedirectProxy.createProxy(ArrayList.class, new MyNewList());
		list.clear();
		
	}
	
	private class MyNewList {
		
		List<String> list = new ArrayList<String>();
		
		public void addString(String s) {
			this.list.add(s);
		}
		
		public int size() {
			return this.list.size()+2;
		}
	}
	
}
