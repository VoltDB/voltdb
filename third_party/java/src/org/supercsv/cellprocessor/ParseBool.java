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
package org.supercsv.cellprocessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Converts a String to a Boolean.
 * <p>
 * The default values for true are: <tt>"true", "1", "y", "t"</tt>
 * <p>
 * The default values for false are: <tt>"false", "0", "n", "f"</tt>
 * <p>
 * The input is converted to lowercase before comparison against the true/false values (to handle all variations of case
 * in the input), so if you supply your own true/false values then ensure they are lowercase.
 * 
 * @author Kasper B. Graversen
 * @author Dominique De Vito
 * @author James Bassett
 * @since 1.0
 */
public class ParseBool extends CellProcessorAdaptor implements StringCellProcessor {
	
	private static final String[] DEFAULT_TRUE_VALUES = new String[] { "1", "true", "t", "y" };
	private static final String[] DEFAULT_FALSE_VALUES = new String[] { "0", "false", "f", "n" };
	
	private final Set<String> trueValues = new HashSet<String>();
	private final Set<String> falseValues = new HashSet<String>();
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the default values.
	 */
	public ParseBool() {
		this(DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES);
	}
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the default values,
	 * then calls the next processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public ParseBool(final BoolCellProcessor next) {
		this(DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, next);
	}
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the supplied true/false
	 * values.
	 * 
	 * @param trueValue
	 *            the String which represents true
	 * @param falseValue
	 *            the String which represents false
	 * @throws NullPointerException
	 *             if trueValue or falseValue is null
	 */
	public ParseBool(final String trueValue, final String falseValue) {
		super();
		checkPreconditions(trueValue, falseValue);
		trueValues.add(trueValue);
		falseValues.add(falseValue);
	}
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the supplied true/false
	 * values.
	 * 
	 * @param trueValues
	 *            the array of Strings which represent true
	 * @param falseValues
	 *            the array of Strings which represent false
	 * @throws IllegalArgumentException
	 *             if trueValues or falseValues is empty
	 * @throws NullPointerException
	 *             if trueValues or falseValues is null
	 */
	public ParseBool(final String[] trueValues, final String[] falseValues) {
		super();
		checkPreconditions(trueValues, falseValues);
		Collections.addAll(this.trueValues, trueValues);
		Collections.addAll(this.falseValues, falseValues);
	}
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the supplied true/false
	 * values, then calls the next processor in the chain.
	 * 
	 * @param trueValue
	 *            the String which represents true
	 * @param falseValue
	 *            the String which represents false
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if trueValue, falseValue or next is null
	 */
	public ParseBool(final String trueValue, final String falseValue, final BoolCellProcessor next) {
		super(next);
		checkPreconditions(trueValue, falseValue);
		trueValues.add(trueValue);
		falseValues.add(falseValue);
	}
	
	/**
	 * Constructs a new <tt>ParseBool</tt> processor, which converts a String to a Boolean using the supplied true/false
	 * values, then calls the next processor in the chain.
	 * 
	 * @param trueValues
	 *            the array of Strings which represent true
	 * @param falseValues
	 *            the array of Strings which represent false
	 * @param next
	 *            the next processor in the chain
	 * @throws IllegalArgumentException
	 *             if trueValues or falseValues is empty
	 * @throws NullPointerException
	 *             if trueValues, falseValues, or next is null
	 */
	public ParseBool(final String[] trueValues, final String[] falseValues, final BoolCellProcessor next) {
		super(next);
		checkPreconditions(trueValues, falseValues);
		Collections.addAll(this.trueValues, trueValues);
		Collections.addAll(this.falseValues, falseValues);
	}
	
	/**
	 * Checks the preconditions for constructing a new ParseBool processor.
	 * 
	 * @param trueValue
	 *            the String which represents true
	 * @param falseValue
	 *            the String which represents false
	 * @throws NullPointerException
	 *             if trueValue or falseValue is null
	 */
	private static void checkPreconditions(final String trueValue, final String falseValue) {
		if( trueValue == null ) {
			throw new NullPointerException("trueValue should not be null");
		}
		if( falseValue == null ) {
			throw new NullPointerException("falseValue should not be null");
		}
	}
	
	/**
	 * Checks the preconditions for constructing a new ParseBool processor.
	 * 
	 * @param trueValues
	 *            the array of Strings which represent true
	 * @param falseValues
	 *            the array of Strings which represent false
	 * @throws IllegalArgumentException
	 *             if trueValues or falseValues is empty
	 * @throws NullPointerException
	 *             if trueValues or falseValues is null
	 */
	private static void checkPreconditions(final String[] trueValues, final String[] falseValues) {
		
		if( trueValues == null ) {
			throw new NullPointerException("trueValues should not be null");
		} else if( trueValues.length == 0 ) {
			throw new IllegalArgumentException("trueValues should not be empty");
		}
		
		if( falseValues == null ) {
			throw new NullPointerException("falseValues should not be null");
		} else if( falseValues.length == 0 ) {
			throw new IllegalArgumentException("falseValues should not be empty");
		}
		
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null, not a String, or can't be parsed to a Boolean
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( !(value instanceof String) ) {
			throw new SuperCsvCellProcessorException(String.class, value, context, this);
		}
		
		final String stringValue = ((String) value).toLowerCase();
		final Boolean result;
		if( trueValues.contains(stringValue) ) {
			result = Boolean.TRUE;
		} else if( falseValues.contains(stringValue) ) {
			result = Boolean.FALSE;
		} else {
			throw new SuperCsvCellProcessorException(String.format("'%s' could not be parsed as a Boolean", value),
				context, this);
		}
		
		return next.execute(result, context);
	}
	
}
