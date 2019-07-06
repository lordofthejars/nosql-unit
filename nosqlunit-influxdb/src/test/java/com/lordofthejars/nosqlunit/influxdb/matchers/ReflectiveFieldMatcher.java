
package com.lordofthejars.nosqlunit.influxdb.matchers;

import java.lang.reflect.Field;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class ReflectiveFieldMatcher<T> extends TypeSafeDiagnosingMatcher<T> {

    private final String propertyName;

    private final Matcher<Object> valueMatcher;

    public ReflectiveFieldMatcher(final String fieldName, final Matcher<?> valueMatcher) {
        this.propertyName = fieldName;
        this.valueMatcher = nastyGenericsWorkaround(valueMatcher);
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("hasProperty(").appendValue(propertyName).appendText(", ")
        .appendDescriptionOf(valueMatcher).appendText(")");
    }

    @SuppressWarnings("unchecked")
    private static Matcher<Object> nastyGenericsWorkaround(final Matcher<?> valueMatcher) {
        return (Matcher<Object>) valueMatcher;
    }

    @Override
    protected boolean matchesSafely(final T obj, final Description mismatch) {
        for (final Field field : obj.getClass().getDeclaredFields()) {
            if (field.getName().equals(propertyName)) {
                field.setAccessible(true);
                try {
                    final Object value = field.get(obj);
                    final boolean match = valueMatcher.matches(value);
                    if (!match) {
                        valueMatcher.describeMismatch(value, mismatch);
                    }
                    return match;
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        mismatch.appendText("No property \"" + propertyName + "\"");
        return false;
    }
}
