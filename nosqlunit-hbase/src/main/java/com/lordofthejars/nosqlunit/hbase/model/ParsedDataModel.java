package com.lordofthejars.nosqlunit.hbase.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedDataModel {

	private String name;
	private List<ParsedColumnFamilyModel> columnFamilies = new ArrayList<ParsedColumnFamilyModel>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ParsedColumnFamilyModel> getColumnFamilies() {
		return columnFamilies;
	}
	public void setColumnFamilies(List<ParsedColumnFamilyModel> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}
	
	
}
