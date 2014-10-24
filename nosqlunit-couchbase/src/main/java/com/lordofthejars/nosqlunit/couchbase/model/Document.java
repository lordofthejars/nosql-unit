package com.lordofthejars.nosqlunit.couchbase.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a document
 */
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Document {

    // This is the content of the doc, in case is a json will be a map, otherwise a String
    private Object document;

    private Integer expirationMSecs;

    public int calculateExpiration() {
        return expirationMSecs == null ?
                0 :
                (int) (System.currentTimeMillis() + expirationMSecs) / 1000;
    }
}
