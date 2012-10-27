package com.lordofthejars.nosqlunit.hbase.model;

import java.io.InputStream;

public interface DataSetParser {

	ParsedDataModel parse(InputStream inputStream);

}
