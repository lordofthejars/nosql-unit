
package com.lordofthejars.nosqlunit.influxdb;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

/**
 * Represents the point data
 *
 * @author Faisal Feroz
 *
 */
public class ExpectedPoint implements Map<String, Object> {

    private final LinkedHashMap<String, Object> pointAsMap = new LinkedHashMap<>();

    public String getMeasurement() {
        return get("measurement", String.class);
    }

    public void setMeasurement(final String measurement) {
        pointAsMap.put("measurement", measurement);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getTags() {
        return get("tags", Map.class);
    }

    public void setTags(final Map<String, String> tags) {
        pointAsMap.put("tags", tags);
    }

    public Long getTime() {
        return get("time", Long.class);
    }

    public void setTime(final Long time) {
        pointAsMap.put("time", time);
    }

    public TimeUnit getPrecision() {
        String precision = get("precision", String.class);
        if (precision != null) {
            return TimeUnit.valueOf(precision);
        }
        return null;
    }

    public void setPrecision(final TimeUnit precision) {
        pointAsMap.put("precision", precision.name());
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getFields() {
        return get("fields", Map.class);
    }

    public void setFields(final Map<String, Object> fields) {
        pointAsMap.put("fields", fields);
    }

    /**
     * Returns the {@link Point} instance
     *
     * @return {@link Point} instance
     */
    public Point toPoint() {
        final Builder builder = Point.measurement(getMeasurement()) //
                .time(getTime(), getPrecision()) //
                .fields(getFields());
        if (getTags() != null) {
            builder.tag(getTags());
        }
        return builder.build();
    }

    /**
     * Gets the value of the given key, casting it to the given {@code Class<T>}. This is
     * useful to avoid having casts in client code, though the effect is the same. So to
     * get the value of a key that is of type String, you would write {@code String name =
     * doc.get("name", String.class)} instead of
     * {@code String name = (String) doc.get("x") }.
     *
     * @param key the key
     * @param clazz the non-null class to cast the value to
     * @param <T> the type of the class
     * @return the value of the given key, or null if the instance does not contain this
     *         key.
     * @throws ClassCastException if the value of the given key is not of type T
     */
    public <T> T get(final Object key, final Class<T> clazz) {
        return clazz.cast(pointAsMap.get(key));
    }

    // Vanilla Map methods delegate to map field

    @Override
    public int size() {
        return pointAsMap.size();
    }

    @Override
    public boolean isEmpty() {
        return pointAsMap.isEmpty();
    }

    @Override
    public boolean containsValue(final Object value) {
        return pointAsMap.containsValue(value);
    }

    @Override
    public boolean containsKey(final Object key) {
        return pointAsMap.containsKey(key);
    }

    @Override
    public Object get(final Object key) {
        return pointAsMap.get(key);
    }

    @Override
    public Object put(final String key, final Object value) {
        return pointAsMap.put(key, value);
    }

    @Override
    public Object remove(final Object key) {
        return pointAsMap.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> map) {
        pointAsMap.putAll(map);
    }

    @Override
    public void clear() {
        pointAsMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return pointAsMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return pointAsMap.values();
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return pointAsMap.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExpectedPoint point = (ExpectedPoint) o;

        return pointAsMap.equals(point.pointAsMap);
    }

    @Override
    public int hashCode() {
        return pointAsMap.hashCode();
    }

    @Override
    public String toString() {
        return "{" + pointAsMap + "}";
    }
}
