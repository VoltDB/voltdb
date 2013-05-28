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

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.util.CsvContext;

/**
 * Exception thrown by CellProcessors when constraint validation fails.
 * <p>
 * Prior to 2.0.0, there was no way to distinguish between constraint validation failures and other exceptions thrown
 * during CellProcessor execution - this class exists for that purpose.
 * 
 * @author James Bassett
 * @since 2.0.0
 */
public class SuperCsvConstraintViolationException extends SuperCsvCellProcessorException {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructs a new <tt>SuperCsvConstraintViolationException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param context
	 *            the CSV context
	 * @param processor
	 *            the cell processor that was executing
	 */
	public SuperCsvConstraintViolationException(final String msg, final CsvContext context,
		final CellProcessor processor) {
		super(msg, context, processor);
	}
	
	/**
	 * Constructs a new <tt>SuperCsvConstraintViolationException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param context
	 *            the CSV context
	 * @param processor
	 *            the cell processor that was executing
	 * @param t
	 *            the nested exception
	 */
	public SuperCsvConstraintViolationException(final String msg, final CsvContext context,
		final CellProcessor processor, final Throwable t) {
		super(msg, context, processor, t);
	}
	
}
