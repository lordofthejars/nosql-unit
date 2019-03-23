package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.annotation.Selective;

import java.lang.annotation.Annotation;

public class SelectiveAnnotationTest implements Annotation, Selective {

    private String identifier;

    private String[] locations;

    public SelectiveAnnotationTest(String identifier, String[] locations) {
        this.identifier = identifier;
        this.locations = locations;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return Selective.class;
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public String[] locations() {
        return locations;
    }


}
