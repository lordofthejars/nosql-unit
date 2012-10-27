package com.lordofthejars.nosqlunit.hbase.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedRowModel {

	private String key;
	private List<ParsedColumnModel> columns = new ArrayList<ParsedColumnModel>();
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public List<ParsedColumnModel> getColumns() {
		return columns;
	}
	public void setColumns(List<ParsedColumnModel> columns) {
		this.columns = columns;
	}
	
}
