package com.lordofthejars.nosqlunit.hbase.model;

import org.apache.hadoop.hbase.util.Bytes;

public class ParsedColumnModel {

    private String name;
    private String value;
    private String valueType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public byte[] getValueInBytes() {
        if ("Integer".equals(valueType)) {
            return Bytes.toBytes(Integer.parseInt(value));
        }
        if ("Long".equals(valueType)) {
            return Bytes.toBytes(Long.parseLong(value));
        }
        if ("Double".equals(valueType)) {
            return Bytes.toBytes(Double.parseDouble(value));
        }
        if ("Float".equals(valueType)) {
            return Bytes.toBytes(Float.parseFloat(value));
        }
        if ("Short".equals(valueType)) {
            return Bytes.toBytes(Short.parseShort(value));
        }
        if ("Boolean".equals(valueType)) {
            return Bytes.toBytes(Boolean.valueOf(value));
        }
        return Bytes.toBytes(value);
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

}
