package com.lordofthejars.nosqlunit.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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

   public static <D> Object callMethod(D instance, Method method, Object[] arguments) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
      Method toInvoke = method;
      //do some sanity checks in case the instance doesn't inherit from the super class
      if(instance.getClass() != method.getDeclaringClass()) {
         toInvoke = instance.getClass().getDeclaredMethod(method.getName(), toClasses(arguments));
      }
      toInvoke.setAccessible(true);
      return toInvoke.invoke(instance, arguments);
   }

   private static Class[] toClasses(Object[] objects) {
      Class[] empty = new Class[0];
      if(objects == null) {
         return empty;
      }
      List<Class> result = new ArrayList<>();
      for(Object object:objects) {
         if(object != null){
            result.add(object.getClass());
         }
      }
      return result.toArray(empty);
   }
}
