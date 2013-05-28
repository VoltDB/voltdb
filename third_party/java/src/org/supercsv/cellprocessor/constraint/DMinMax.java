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
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * Converts the input data to a Double and ensures that number is within a specified numeric range (inclusive). If the
 * data has no upper bound (or lower bound), you should use either of <code>MIN</code> or <code>MAX</code> constants
 * provided in the class.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class DMinMax extends CellProcessorAdaptor {
	
	/** Maximum value for a Double */
	public static final double MAX_DOUBLE = Double.MAX_VALUE;
	
	/** Minimum value for a Double */
	public static final double MIN_DOUBLE = Double.MIN_VALUE;
	
	/** Maximum value for a Short */
	public static final double MAX_SHORT = Short.MAX_VALUE;
	
	/** Minimum value for a Short */
	public static final double MIN_SHORT = Short.MIN_VALUE;
	
	/** Maximum value for a Character */
	public static final double MAX_CHAR = Character.MAX_VALUE;
	
	/** Minimum value for a Character */
	public static final double MIN_CHAR = Character.MIN_VALUE;
	
	/** Maximum value for 8 bits (unsigned) */
	public static final int MAX_8_BIT_UNSIGNED = 255;
	
	/** Minimum value for 8 bits (unsigned) */
	public static final int MIN_8_BIT_UNSIGNED = 0;
	
	/** Maximum value for 8 bits (signed) */
	public static final int MAX_8_BIT_SIGNED = Byte.MAX_VALUE;
	
	/** Minimum value for 8 bits (signed) */
	public static final int MIN_8_BIT_SIGNED = Byte.MIN_VALUE;
	
	private final double min;
	
	private final double max;
	
	/**
	 * Constructs a new <tt>DMinMax</tt> processor, which converts the input to a Double and ensures the value is
	 * between the supplied min and max values.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @throws IllegalArgumentException
	 *             if max < min
	 */
	public DMinMax(final double min, final double max) {
		super();
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Constructs a new <tt>DMinMax</tt> processor, which converts the input to a Double, ensures the value is between
	 * the supplied min and max values, then calls the next processor in the chain.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @param next
	 *            the next processor in the chain
	 * @throws IllegalArgumentException
	 *             if max < min
	 * @throws NullPointerException
	 *             if next is null
	 */
	public DMinMax(final double min, final double max, final DoubleCellProcessor next) {
		super(next);
		checkPreconditions(min, max);
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Checks the preconditions for creating a new DMinMax processor.
	 * 
	 * @param min
	 *            the minimum value (inclusive)
	 * @param max
	 *            the maximum value (inclusive)
	 * @throws IllegalArgumentException
	 *             if max < min
	 */
	private static void checkPreconditions(final double min, final double max) {
		if( max < min ) {
			throw new IllegalArgumentException(String.format("max (%f) should not be < min (%f)", max, min));
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null or can't be parsed as a Double
	 * @throws SuperCsvConstraintViolationException
	 *             if value doesn't lie between min and max (inclusive)
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final Double result;
		if( value instanceof Double ) {
			result = (Double) value;
		} else {
			try {
				result = Double.parseDouble(value.toString());
			}
			catch(final NumberFormatException e) {
				throw new SuperCsvCellProcessorException(String.format("'%s' could not be parsed as a Double", value),
					context, this, e);
			}
		}
		
		if( result < min || result > max ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"%f does not lie between the min (%f) and max (%f) values (inclusive)", result, min, max), context,
				this);
		}
		
		return next.execute(result, context);
	}
	
}
