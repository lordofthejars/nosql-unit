package com.lordofthejars.nosqlunit.marklogic.content;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicRule.EXPECTED_RESERVED_WORD;

/**
 * Implements the logic required to handle unstructured documents, like binaries and
 * plain-text docs which should not be touched by the test framework and have no possibility to
 * add the database-specific  content.
 */
public class PassThroughParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(PassThroughParser.class);

    private Class<?> resourceBase;

    public PassThroughParser(Object target) {
        if (target != null) {
            resourceBase = target.getClass();
        }
    }

    public Set<Content> parse(InputStream is) {
        Set<Content> result = new HashSet<>();
        result.add(new PassThroughContent(parseUri(is), is));
        return result;
    }

    private String parseUri(InputStream is) {
        String result = null;
        if (is instanceof FilterInputStream) {
            Object in = Reflections.accessField(is.getClass(), is, "in");
            if (in instanceof FileInputStream) {
                Object p = Reflections.accessField(in.getClass(), in, "path");
                if (p != null && resourceBase != null) {
                    String resourcesPath = p.toString().replace("\\", "/");
                    String basePath = resourceBase.getResource(".").getPath();
                    int startIndex = (basePath.startsWith("/") ? basePath.length() - 1 : basePath.length()) - 1;
                    result = resourcesPath.substring(startIndex).replace(EXPECTED_RESERVED_WORD, "");
                }
            }
        }
        LOGGER.info("URI: {}", result);
        return result;
    }

    private static abstract class Reflections {

        static Object accessField(Class clazz, Object o, String fieldName) {
            try {
                if (clazz == Object.class) {
                    throw new IllegalArgumentException("reached bottom, no field \"" + fieldName + "\" found");
                }
                Field field = getDeclaredField(clazz, fieldName);
                if (field == null) {
                    return accessField(clazz.getSuperclass(), o, fieldName);
                }
                field.setAccessible(true);
                return field.get(o);
            } catch (Exception e) {
                LOGGER.debug(e.getMessage(), e);
                return null;
            }
        }

        static Field getDeclaredField(Class clazz, String fieldName) {
            try {
                return clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                return null;
            }
        }
    }
}
