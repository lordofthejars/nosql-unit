package com.lordofthejars.nosqlunit.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectionUtil {

   public static <T> T createInstance(Class<?> clazz) {
      try {
         return (T) clazz.newInstance();
      } catch (InstantiationException e) {
         throw new IllegalArgumentException(e);
      } catch (IllegalAccessException e) {
         throw new IllegalArgumentException(e);
      }
   }

   public static <T> T createInstance(String clazz) {
      try {
         return (T) Class.forName(clazz).newInstance();
      } catch (InstantiationException e) {
         throw new IllegalArgumentException(e);
      } catch (IllegalAccessException e) {
         throw new IllegalArgumentException(e);
      } catch (ClassNotFoundException e) {
         throw new IllegalArgumentException(e);
      }
   }

   public static <D> Object callMethod(D instance, Method method, Object[] arguments) throws InvocationTargetException, IllegalAccessException {
      method.setAccessible(true);
      return method.invoke(instance, arguments);
   }

}
