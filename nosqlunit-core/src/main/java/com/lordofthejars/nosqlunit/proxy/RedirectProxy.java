package com.lordofthejars.nosqlunit.proxy;

import static org.joor.Reflect.on;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.joor.ReflectException;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class RedirectProxy<S, D> {
	
	@SuppressWarnings("unchecked")
	public static <S, D> S createProxy(Class<S> source, D destination, Object ... arguments) {
		return (S) Enhancer.create(source, new RedirectMethod(destination));
	}

	public static class RedirectMethod<D> implements MethodInterceptor {

		private D destination;

		public RedirectMethod(D destination) {
			this.destination = destination;
		}

		public Object intercept(Object object, Method method, Object[] arguments, MethodProxy proxy) throws Throwable {
			try {

				return on(destination).call(method.getName(), arguments).get();

			} catch (ReflectException e) {
				throw new UnsupportedOperationException("The method " + method.getName() + " with parameters "
						+ Arrays.toString(arguments) + " does not exist on class " + destination.getClass());
			}
		}

	}

}
