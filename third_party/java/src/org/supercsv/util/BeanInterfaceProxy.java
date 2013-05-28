/*
 * Copyright 2007 Kasper B. Graversen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.supercsv.util;

import static org.supercsv.util.ReflectionUtils.GET_PREFIX;
import static org.supercsv.util.ReflectionUtils.SET_PREFIX;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * This is part of the internal implementation of Super CSV.
 * <p>
 * This class creates bean instances based on an interface. This allows you, given an interface for a bean (but no
 * implementation), to generate a bean implementation on-the-fly. This instance can then be used for fetching and
 * storing state. It assumes all get methods starts with "get" and all set methods start with "set" and takes only 1
 * argument.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public final class BeanInterfaceProxy implements InvocationHandler {
	
	private final Map<String, Object> beanState = new HashMap<String, Object>();
	
	// no instantiation
	private BeanInterfaceProxy() {
	}
	
	/**
	 * Creates a proxy object which implements a given bean interface.
	 * 
	 * @param proxyInterface
	 *            the interface the the proxy will implement
	 * @return the proxy implementation
	 * @throws NullPointerException
	 *             if proxyInterface is null
	 */
	public static <T> T createProxy(final Class<T> proxyInterface) {
		if( proxyInterface == null ) {
			throw new NullPointerException("proxyInterface should not be null");
		}
		return proxyInterface.cast(Proxy.newProxyInstance(proxyInterface.getClassLoader(),
			new Class[] { proxyInterface }, new BeanInterfaceProxy()));
	}
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * If a getter method is encountered then this method returns the stored value from the bean state (or null if the
	 * field has not been set).
	 * <p>
	 * If a setter method is encountered then the bean state is updated with the value of the first argument and the
	 * value is returned (to allow for method chaining)
	 * 
	 * @throws IllegalArgumentException
	 *             if the method is not a valid getter/setter
	 */
	public Object invoke(final Object proxy, final Method method, final Object[] args) {
		
		final String methodName = method.getName();
		
		if( methodName.startsWith(GET_PREFIX) ) {
			
			if( method.getParameterTypes().length > 0 ) {
				throw new IllegalArgumentException(String.format(
					"method %s.%s() should have no parameters to be a valid getter", method.getDeclaringClass()
						.getName(), methodName));
			}
			
			// simulate getter by retrieving value from bean state
			return beanState.get(methodName.substring(GET_PREFIX.length()));
			
		} else if( methodName.startsWith(SET_PREFIX) ) {
			
			if( args == null || args.length != 1 ) {
				throw new IllegalArgumentException(String.format(
					"method  %s.%s() should have exactly one parameter to be a valid setter", method
						.getDeclaringClass().getName(), methodName));
			}
			
			// simulate setter by storing value in bean state
			beanState.put(methodName.substring(SET_PREFIX.length()), args[0]);
			return proxy;
			
		} else {
			throw new IllegalArgumentException(String.format("method %s.%s() is not a valid getter/setter", method
				.getDeclaringClass().getName(), methodName));
		}
		
	}
}
