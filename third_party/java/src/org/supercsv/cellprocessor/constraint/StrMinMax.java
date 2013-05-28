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
package org.supercsv.cellprocessor.constraint;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * This constraint ensures that the input data has a string length between the supplied min and max values (both
 * inclusive). Should the input be anything different from a String, it will be converted to a string using the input's
 * <code>toString()</code> method.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class StrMinMax extends CellProcessorAdaptor implements StringCellProcessor {
	
	private final long min;
	private final long max;
	
	/**
	 * Constructs a new <tt>StrMinMax</tt> processor, which ensures that the input data has a string length between the
	 * supplied min and max values (both inclusive).
	 * 
	 * @param min
	 *            the minimum String length
	 * @param max
	 *            the maximum String length
	 * @throws IllegalArgumentException
	 *             if max < min, or min is < 0
	 */
	public StrMinMax(final long min, final long max) {
		super();
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Constructs a new <tt>StrMinMax</tt> processor, which ensures that the input data has a string length between the
	 * supplied min and max values (both inclusive), then calls the next processor in the chain.
	 * 
	 * @param min
	 *            the minimum String length
	 * @param max
	 *            the maximum String length
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 * @throws IllegalArgumentException
	 *             if max < min, or min is < 0
	 */
	public StrMinMax(final long min, final long max, final CellProcessor next) {
		super(next);
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Checks the preconditions for creating a new StrMinMax processor.
	 * 
	 * @param min
	 *            the minimum String length
	 * @param max
	 *            the maximum String length
	 * @throws IllegalArgumentException
	 *             if max < min, or min is < 0
	 */
	private static void checkPreconditions(final long min, final long max) {
		if( max < min ) {
			throw new IllegalArgumentException(String.format("max (%d) should not be < min (%d)", max, min));
		}
		if( min < 0 ) {
			throw new IllegalArgumentException(String.format("min length (%d) should not be < 0", min));
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if length is < min or length > max
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final String stringValue = value.toString();
		final int length = stringValue.length();
		if( length < min || length > max ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"the length (%d) of value '%s' does not lie between the min (%d) and max (%d) values (inclusive)",
				length, stringValue, min, max), context, this);
		}
		
		return next.execute(stringValue, context);
	}
}
