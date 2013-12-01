package com.lordofthejars.nosqlunit.hbase.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

public class ParsedRowModel {
    
    private String key;
    private String keyType;
    private List<ParsedColumnModel> columns = new ArrayList<ParsedColumnModel>();
    
    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }
    
    public byte[] getKeyInBytes() {
        if ("Integer".equals(keyType)) {
            return Bytes.toBytes(Integer.parseInt(key));
        }
        if ("Long".equals(keyType)) {
            return Bytes.toBytes(Long.parseLong(key));
        }
        if ("Double".equals(keyType)) {
            return Bytes.toBytes(Double.parseDouble(key));
        }
        if ("Float".equals(keyType)) {
            return Bytes.toBytes(Float.parseFloat(key));
        }
        if ("Short".equals(keyType)) {
            return Bytes.toBytes(Short.parseShort(key));
        }
        return Bytes.toBytes(key);
    }
    
    public String getKeyType() {
        return keyType;
    }
    
    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }
    
    public List<ParsedColumnModel> getColumns() {
        return columns;
    }
    
    public void setColumns(List<ParsedColumnModel> columns) {
        this.columns = columns;
    }
    
}
