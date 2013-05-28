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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.supercsv.exception.SuperCsvReflectionException;

/**
 * Provides useful utility methods for reflection.
 * 
 * @author James Bassett
 * @since 2.0.0
 */
public final class ReflectionUtils {
	
	public static final String SET_PREFIX = "set";
	public static final String GET_PREFIX = "get";
	public static final String IS_PREFIX = "is";
	
	/**
	 * A map of primitives and their associated wrapper classes, to cater for autoboxing.
	 */
	private static final Map<Class<?>, Class<?>> AUTOBOXING_CONVERTER = new HashMap<Class<?>, Class<?>>();
	static {
		AUTOBOXING_CONVERTER.put(long.class, Long.class);
		AUTOBOXING_CONVERTER.put(Long.class, long.class);
		AUTOBOXING_CONVERTER.put(int.class, Integer.class);
		AUTOBOXING_CONVERTER.put(Integer.class, int.class);
		AUTOBOXING_CONVERTER.put(char.class, Character.class);
		AUTOBOXING_CONVERTER.put(Character.class, char.class);
		AUTOBOXING_CONVERTER.put(byte.class, Byte.class);
		AUTOBOXING_CONVERTER.put(Byte.class, byte.class);
		AUTOBOXING_CONVERTER.put(short.class, Short.class);
		AUTOBOXING_CONVERTER.put(Short.class, short.class);
		AUTOBOXING_CONVERTER.put(boolean.class, Boolean.class);
		AUTOBOXING_CONVERTER.put(Boolean.class, boolean.class);
		AUTOBOXING_CONVERTER.put(double.class, Double.class);
		AUTOBOXING_CONVERTER.put(Double.class, double.class);
		AUTOBOXING_CONVERTER.put(float.class, Float.class);
		AUTOBOXING_CONVERTER.put(Float.class, float.class);
	}
	
	// no instantiation
	private ReflectionUtils() {
	}
	
	/**
	 * Returns the getter method associated with the object's field.
	 * 
	 * @param object
	 *            the object
	 * @param fieldName
	 *            the name of the field
	 * @return the getter method
	 * @throws NullPointerException
	 *             if object or fieldName is null
	 * @throws SuperCsvReflectionException
	 *             if the getter doesn't exist or is not visible
	 */
	public static Method findGetter(final Object object, final String fieldName) {
		if( object == null ) {
			throw new NullPointerException("object should not be null");
		} else if( fieldName == null ) {
			throw new NullPointerException("fieldName should not be null");
		}
		
		final Class<?> clazz = object.getClass();
		
		// find a standard getter
		final String standardGetterName = getMethodNameForField(GET_PREFIX, fieldName);
		Method getter = findGetterWithCompatibleReturnType(standardGetterName, clazz, false);
		
		// if that fails, try for an isX() style boolean getter
		if( getter == null ) {
			final String booleanGetterName = getMethodNameForField(IS_PREFIX, fieldName);
			getter = findGetterWithCompatibleReturnType(booleanGetterName, clazz, true);
		}
		
		if( getter == null ) {
			throw new SuperCsvReflectionException(
				String
					.format(
						"unable to find getter for field %s in class %s - check that the corresponding nameMapping element matches the field name in the bean",
						fieldName, clazz.getName()));
		}
		
		return getter;
	}
	
	/**
	 * Helper method for findGetter() that finds a getter with the supplied name, optionally enforcing that the method
	 * must have a Boolean/boolean return type. Developer note: this method could have accepted an actual return type to
	 * enforce, but it was more efficient to cater for only Booleans (as they're the only type that has differently
	 * named getters).
	 * 
	 * @param getterName
	 *            the getter name
	 * @param clazz
	 *            the class
	 * @param enforceBooleanReturnType
	 *            if true, the method must return a Boolean/boolean, otherwise it's return type doesn't matter
	 * @return the getter, or null if none is found
	 */
	private static Method findGetterWithCompatibleReturnType(final String getterName, final Class<?> clazz,
		final boolean enforceBooleanReturnType) {
		
		for( final Method method : clazz.getMethods() ) {
			
			if( !getterName.equals(method.getName()) || method.getParameterTypes().length != 0
				|| method.getReturnType().equals(void.class) ) {
				continue; // getter must have correct name, 0 parameters and a return type
			}
			
			if( !enforceBooleanReturnType || boolean.class.equals(method.getReturnType())
				|| Boolean.class.equals(method.getReturnType()) ) {
				return method;
			}
			
		}
		
		return null;
	}
	
	/**
	 * Returns the setter method associated with the object's field.
	 * <p>
	 * This method handles any autoboxing/unboxing of the argument passed to the setter (e.g. if the setter type is a
	 * primitive {@code int} but the argument passed to the setter is an {@code Integer}) by looking for a setter with
	 * the same type, and failing that checking for a setter with the corresponding primitive/wrapper type.
	 * <p>
	 * It also allows for an argument type that is a subclass or implementation of the setter type (when the setter type
	 * is an {@code Object} or {@code interface} respectively).
	 * 
	 * @param object
	 *            the object
	 * @param fieldName
	 *            the name of the field
	 * @param argumentType
	 *            the type to be passed to the setter
	 * @return the setter method
	 * @throws NullPointerException
	 *             if object, fieldName or fieldType is null
	 * @throws SuperCsvReflectionException
	 *             if the setter doesn't exist or is not visible
	 */
	public static Method findSetter(final Object object, final String fieldName, final Class<?> argumentType) {
		if( object == null ) {
			throw new NullPointerException("object should not be null");
		} else if( fieldName == null ) {
			throw new NullPointerException("fieldName should not be null");
		} else if( argumentType == null ) {
			throw new NullPointerException("argumentType should not be null");
		}
		
		final String setterName = getMethodNameForField(SET_PREFIX, fieldName);
		final Class<?> clazz = object.getClass();
		
		// find a setter compatible with the supplied argument type
		Method setter = findSetterWithCompatibleParamType(clazz, setterName, argumentType);
		
		// if that failed, try the corresponding primitive/wrapper if it's a type that can be autoboxed/unboxed
		if( setter == null && AUTOBOXING_CONVERTER.containsKey(argumentType) ) {
			setter = findSetterWithCompatibleParamType(clazz, setterName, AUTOBOXING_CONVERTER.get(argumentType));
		}
		
		if( setter == null ) {
			throw new SuperCsvReflectionException(
				String
					.format(
						"unable to find method %s(%s) in class %s - check that the corresponding nameMapping element matches the field name in the bean, "
							+ "and the cell processor returns a type compatible with the field", setterName,
						argumentType.getName(), clazz.getName()));
		}
		
		return setter;
	}
	
	/**
	 * Helper method for findSetter() that returns the setter method of the supplied name, whose parameter type is
	 * compatible with the supplied argument type (will allow an object of that type to be used when invoking the
	 * setter), or returns <tt>null</tt> if no match is found. Preference is given to setters whose parameter type is an
	 * exact match, but if there is none, then the first compatible method found is returned.
	 * 
	 * @param clazz
	 *            the class containing the setter
	 * @param setterName
	 *            the name of the setter
	 * @param argumentType
	 *            the type to be passed to the setter
	 * @return the setter method, or null if none is found
	 */
	private static Method findSetterWithCompatibleParamType(final Class<?> clazz, final String setterName,
		final Class<?> argumentType) {
		
		Method compatibleSetter = null;
		for( final Method method : clazz.getMethods() ) {
			
			if( !setterName.equals(method.getName()) || method.getParameterTypes().length != 1 ) {
				continue; // setter must have correct name and only 1 parameter
			}
			
			final Class<?> parameterType = method.getParameterTypes()[0];
			if( parameterType.equals(argumentType) ) {
				compatibleSetter = method;
				break; // exact match
				
			} else if( parameterType.isAssignableFrom(argumentType) ) {
				compatibleSetter = method; // potential match, but keep looking for exact match
			}
			
		}
		
		return compatibleSetter;
	}
	
	/**
	 * Gets the camelcase getter/setter method name for a field.
	 * 
	 * @param prefix
	 *            the method prefix
	 * @param fieldName
	 *            the field name
	 * @return the method name
	 */
	private static String getMethodNameForField(final String prefix, final String fieldName) {
		return prefix + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}
}
