package com.lordofthejars.nosqlunit.demo.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor // Jackson required
@AllArgsConstructor
public class Book {

	private String title;
	
	private int numberOfPages;
}
