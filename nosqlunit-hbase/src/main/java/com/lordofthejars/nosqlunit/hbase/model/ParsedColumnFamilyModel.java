package com.lordofthejars.nosqlunit.hbase.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedColumnFamilyModel {

	private String name;
	private List<ParsedRowModel> rows = new ArrayList<ParsedRowModel>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ParsedRowModel> getRows() {
		return rows;
	}
	public void setRows(List<ParsedRowModel> rows) {
		this.rows = rows;
	}

}
