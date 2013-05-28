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
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * Converts the input data to a Long and and ensures the value is between the supplied min and max values (inclusive).
 * If the data has no upper or lower bound, you should use either of <code>MIN</code> or <code>MAX</code> constants
 * provided in the class.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class LMinMax extends CellProcessorAdaptor {
	
	/** Maximum value for a Long */
	public static final long MAX_LONG = Long.MAX_VALUE;
	
	/** Minimum value for a Long */
	public static final long MIN_LONG = Long.MIN_VALUE;
	
	/** Maximum value for an Integer */
	public static final int MAX_INTEGER = Integer.MAX_VALUE;
	
	/** Minimum value for an Integer */
	public static final int MIN_INTEGER = Integer.MIN_VALUE;
	
	/** Maximum value for a Short */
	public static final short MAX_SHORT = Short.MAX_VALUE;
	
	/** Minimum value for a Short */
	public static final short MIN_SHORT = Short.MIN_VALUE;
	
	/** Maximum value for a Character */
	public static final int MAX_CHAR = Character.MAX_VALUE;
	
	/** Minimum value for a Character */
	public static final int MIN_CHAR = Character.MIN_VALUE;
	
	/** Maximum value for 8 bits (unsigned) */
	public static final int MAX_8_BIT_UNSIGNED = 255;
	
	/** Minimum value for 8 bits (unsigned) */
	public static final int MIN_8_BIT_UNSIGNED = 0;
	
	/** Maximum value for 8 bits (signed) */
	public static final int MAX_8_BIT_SIGNED = Byte.MAX_VALUE;
	
	/** Minimum value for 8 bits (signed) */
	public static final int MIN_8_BIT_SIGNED = Byte.MIN_VALUE;
	
	private final long min;
	
	private final long max;
	
	/**
	 * Constructs a new <tt>LMinMax</tt> processor, which converts the input data to a Long and and ensures the value is
	 * between the supplied min and max values.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @throws IllegalArgumentException
	 *             if max < min
	 */
	public LMinMax(final long min, final long max) {
		super();
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Constructs a new <tt>LMinMax</tt> processor, which converts the input data to a Long and and ensures the value is
	 * between the supplied min and max values, then calls the next processor in the chain.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 * @throws IllegalArgumentException
	 *             if max < min
	 */
	public LMinMax(final long min, final long max, final LongCellProcessor next) {
		super(next);
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Checks the preconditions for creating a new LMinMax processor.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @throws IllegalArgumentException
	 *             if max < min
	 */
	private static void checkPreconditions(final long min, final long max) {
		if( max < min ) {
			throw new IllegalArgumentException(String.format("max (%d) should not be < min (%d)", max, min));
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null or  can't be parsed as a Long
	 * @throws SuperCsvConstraintViolationException
	 *             if value, or doesn't lie between min and max (inclusive)
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final Long result;
		if( value instanceof Long ) {
			result = (Long) value;
		} else {
			try {
				result = Long.parseLong(value.toString());
			}
			catch(final NumberFormatException e) {
				throw new SuperCsvCellProcessorException(String.format("'%s' could not be parsed as a Long", value), context, this,
					e);
			}
		}
		
		if( result < min || result > max ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"%d does not lie between the min (%d) and max (%d) values (inclusive)", result, min, max), context,
				this);
		}
		
		return next.execute(result, context);
	}
	
}
