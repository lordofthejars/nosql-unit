package com.lordofthejars.nosqlunit.util;

public class CsvBuilder {

	private CsvBuilder() {
		super();
	}
	
	public static final <T> String joinFrom(Iterable<T> elements) {
		
		StringBuilder stringBuilder = new StringBuilder();
		
		for (T element : elements) {
			stringBuilder.append(element.toString()).append(", ");
		}
		
		return stringBuilder.toString().substring(0, stringBuilder.length()-2);
		
	}
	
}
