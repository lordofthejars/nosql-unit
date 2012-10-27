package com.lordofthejars.nosqlunit.demo.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import ch.lambdaj.function.convert.Converter;

public class PersonManager {

	private Configuration configuration;
	
	public PersonManager(Configuration configuration) {
		this.configuration = configuration;		
	}
	
	public String getCarByPersonName(String personName) throws IOException {
		HTable table = new HTable(configuration, "person");
		Get get = new Get("john".getBytes());
		Result result = table.get(get);
		
		return new String(result.getValue(toByteArray().convert("personFamilyName"), toByteArray().convert("car")));
	}
	
	private Converter<String, byte[]> toByteArray() {
		return new Converter<String, byte[]>() {

			@Override
			public byte[] convert(String element) {
				return element.getBytes();
			}
		};
	}
	
}
