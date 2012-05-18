package com.lordofthejars.nosqlunit;

import static org.junit.Assert.*;

import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MyTest {

	
	private String data = "{\"collection1\": [{\"id\":1,\"code\":\"JSON dataset\",},{\"id\":2,\"code\":\"Another row\",}],\"collection2\": [{\"id\":1,\"code\":\"JSON dataset\",},{\"id\":2,\"code\":\"Another row\",}]}";

	@Test
	public void test() {
		DBObject object = (DBObject) JSON.parse(data);
		
		BasicDBList object2 = (BasicDBList)object.get("collection1");
		for (Object object3 : object2) {
			System.out.println(object3);			
		}
	}

}
