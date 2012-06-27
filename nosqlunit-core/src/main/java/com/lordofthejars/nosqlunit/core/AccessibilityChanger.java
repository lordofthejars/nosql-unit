package com.lordofthejars.nosqlunit.core;

import java.lang.reflect.AccessibleObject;

/*
 * Copyright (c) 2007 Mockito contributors
 * This class is made available under the terms of the MIT License.
 */
public class AccessibilityChanger {
    
    private Boolean wasAccessible = null;

    /**
     * safely disables access
     */
    public void safelyDisableAccess(AccessibleObject accessibleObject) {
        assert wasAccessible != null : "accessibility info shall not be null";
        try {
            accessibleObject.setAccessible(wasAccessible);
        } catch (Throwable t) {
            //ignore
        }
    }

    /**
     * changes the accessibleObject accessibility and returns true if accessibility was changed
     */
    public void enableAccess(AccessibleObject accessibleObject) {
        wasAccessible = accessibleObject.isAccessible();
        accessibleObject.setAccessible(true);
    }
}