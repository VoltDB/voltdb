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
package org.supercsv.exception;

/**
 * Wraps the following reflection related checked exceptions:
 * <p>
 * <tt>
 * ClassNotFoundException, IllegalAccessException, InstantiationException, InvocationTargetException,
 * NoSuchMethodException</tt>
 * <p>
 * 
 * @since 1.30
 * @author Kasper B. Graversen
 */
public class SuperCsvReflectionException extends SuperCsvException {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructs a new <tt>SuperCsvReflectionException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 */
	public SuperCsvReflectionException(final String msg) {
		super(msg);
	}
	
	/**
	 * Constructs a new <tt>SuperCsvReflectionException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param t
	 *            the nested exception
	 */
	public SuperCsvReflectionException(final String msg, final Throwable t) {
		super(msg, null, t);
	}
	
}
