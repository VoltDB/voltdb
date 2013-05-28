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

import org.supercsv.util.CsvContext;

/**
 * Generic SuperCSV Exception class. It contains the CSV context (line number, column number and raw line) from when the
 * exception occurred.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class SuperCsvException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	
	private CsvContext csvContext;
	
	/**
	 * Constructs a new <tt>SuperCsvException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 */
	public SuperCsvException(final String msg) {
		super(msg);
	}
	
	/**
	 * Constructs a new <tt>SuperCsvException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param context
	 *            the CSV context
	 */
	public SuperCsvException(final String msg, final CsvContext context) {
		super(msg);
		this.csvContext = context;
	}
	
	/**
	 * Constructs a new <tt>SuperCsvException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param context
	 *            the CSV context
	 * @param t
	 *            the nested exception
	 */
	public SuperCsvException(final String msg, final CsvContext context, final Throwable t) {
		super(msg, t);
		this.csvContext = context;
	}
	
	/**
	 * Gets the current CSV context.
	 * 
	 * @return the current CSV context, or <tt>null</tt> if none is available
	 */
	public CsvContext getCsvContext() {
		return csvContext;
	}
	
	/**
	 * Returns the String representation of this exception.
	 */
	@Override
	public String toString() {
		return String.format("%s: %s%ncontext=%s", getClass().getName(), getMessage(), csvContext);
	}
}
