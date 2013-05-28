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
 * Exception thrown when CellProcessor execution fails (typically due to invalid input) - constraint validating
 * CellProcessors should throw {@link SuperCsvConstraintViolationException} for constraint validation failures.
 * 
 * @author James Bassett
 * @since 2.0.0
 */
public class SuperCsvCellProcessorException extends SuperCsvException {
	
	private static final long serialVersionUID = 1L;
	
	private final CellProcessor processor;
	
	/**
	 * Constructs a new <tt>SuperCsvCellProcessorException</tt>.
	 * 
	 * @param msg
	 *            the exception message
	 * @param context
	 *            the CSV context
	 * @param processor
	 *            the cell processor that was executing
	 */
	public SuperCsvCellProcessorException(final String msg, final CsvContext context, final CellProcessor processor) {
		super(msg, context);
		this.processor = processor;
	}
	
	/**
	 * Constructs a new <tt>SuperCsvCellProcessorException</tt>.
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
	public SuperCsvCellProcessorException(final String msg, final CsvContext context, final CellProcessor processor,
		final Throwable t) {
		super(msg, context, t);
		this.processor = processor;
	}
	
	/**
	 * Constructs a new <tt>SuperCsvCellProcessorException</tt> to indicate that the value received by a CellProcessor
	 * wasn't of the correct type.
	 * 
	 * @param expectedType
	 *            the expected type
	 * @param actualValue
	 *            the value received by the CellProcessor
	 * @param context
	 *            the CSV context
	 * @param processor
	 *            the cell processor that was executing
	 */
	public SuperCsvCellProcessorException(final Class<?> expectedType, final Object actualValue,
		final CsvContext context, final CellProcessor processor) {
		super(getUnexpectedTypeMessage(expectedType, actualValue), context);
		this.processor = processor;
	}
	
	/**
	 * Assembles the exception message when the value received by a CellProcessor isn't of the correct type.
	 * 
	 * @param expectedType
	 *            the expected type
	 * @param actualValue
	 *            the value received by the CellProcessor
	 * @return the message
	 * @throws NullPointerException
	 *             if expectedType is null
	 */
	private static String getUnexpectedTypeMessage(final Class<?> expectedType, final Object actualValue) {
		if( expectedType == null ) {
			throw new NullPointerException("expectedType should not be null");
		}
		String expectedClassName = expectedType.getName();
		String actualClassName = (actualValue != null) ? actualValue.getClass().getName() : "null";
		return String.format("the input value should be of type %s but is %s", expectedClassName, actualClassName);
	}
	
	/**
	 * Gets the processor that was executing.
	 * 
	 * @return the processor that was executing
	 */
	public CellProcessor getProcessor() {
		return processor;
	}
	
	/**
	 * Returns the String representation of this exception.
	 */
	@Override
	public String toString() {
		return String.format("%s: %s%nprocessor=%s%ncontext=%s", getClass().getName(), getMessage(), processor,
			getCsvContext());
	}
	
}
