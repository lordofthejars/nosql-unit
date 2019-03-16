package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.annotation.SelectiveMatcher;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;

import java.lang.annotation.Annotation;

public class ShouldMatchDataSetAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet {

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
