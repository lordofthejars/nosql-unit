
package com.lordofthejars.nosqlunit.dynamodb.integration;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.SelectiveMatcher;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;

public class ShouldMatchDataSetAnnotationTest
        implements Annotation, com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet {

    private String location;

    private SelectiveMatcher[] selectiveMatchers;

    public ShouldMatchDataSetAnnotationTest(String location) {
        this.location = location;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return ShouldMatchDataSet.class;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public SelectiveMatcher[] withSelectiveMatcher() {
        return selectiveMatchers;
    }

}
