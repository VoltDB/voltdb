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

import java.math.BigDecimal;
import java.text.DecimalFormatSymbols;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Convert a String to a BigDecimal. It uses the String constructor of BigDecimal (<tt>new BigDecimal("0.1")</tt>) as it
 * yields predictable results (see {@link BigDecimal}).
 * <p>
 * If the data uses a character other than "." as a decimal separator (France uses "," for example), then use the
 * constructor that accepts a <tt>DecimalFormatSymbols</tt> object, as it will convert the character to a "." before
 * creating the BigDecimal.
 * 
 * @since 1.30
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class ParseBigDecimal extends CellProcessorAdaptor implements StringCellProcessor {
	
	private static final char DEFAULT_DECIMAL_SEPARATOR = '.';
	
	private final char decimalSeparator;
	
	/**
	 * Constructs a new <tt>ParseBigDecimal</tt> processor, which converts a String to a BigDecimal.
	 */
	public ParseBigDecimal() {
		this.decimalSeparator = DEFAULT_DECIMAL_SEPARATOR;
	}
	
	/**
	 * Constructs a new <tt>ParseBigDecimal</tt> processor, which converts a String to a BigDecimal using the supplied
	 * <tt>DecimalFormatSymbols</tt> object to convert any decimal separator to a "." before creating the BigDecimal.
	 * 
	 * @param symbols
	 *            the decimal format symbols, containing the decimal separator
	 * @throws NullPointerException
	 *             if symbols is null
	 */
	public ParseBigDecimal(final DecimalFormatSymbols symbols) {
		super();
		checkPreconditions(symbols);
		this.decimalSeparator = symbols.getDecimalSeparator();
	}
	
	/**
	 * Constructs a new <tt>ParseBigDecimal</tt> processor, which converts a String to a BigDecimal then calls the next
	 * processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public ParseBigDecimal(final CellProcessor next) {
		super(next);
		this.decimalSeparator = DEFAULT_DECIMAL_SEPARATOR;
	}
	
	/**
	 * Constructs a new <tt>ParseBigDecimal</tt> processor, which converts a String to a BigDecimal using the supplied
	 * <tt>DecimalFormatSymbols</tt> object to convert any decimal separator to a "." before creating the BigDecimal,
	 * then calls the next processor in the chain.
	 * 
	 * @param symbols
	 *            the decimal format symbols, containing the decimal separator
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if symbols or next is null
	 */
	public ParseBigDecimal(final DecimalFormatSymbols symbols, final CellProcessor next) {
		super(next);
		checkPreconditions(symbols);
		this.decimalSeparator = symbols.getDecimalSeparator();
	}
	
	/**
	 * Checks the preconditions for creating a new ParseBigDecimal processor.
	 * 
	 * @param symbols
	 *            the decimal format symbols, containing the decimal separator
	 * @throws NullPointerException
	 *             if symbols is null
	 */
	private static void checkPreconditions(final DecimalFormatSymbols symbols) {
		if( symbols == null ) {
			throw new NullPointerException("symbols should not be null");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null, isn't a String, or can't be parsed as a BigDecimal
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final BigDecimal result;
		if( value instanceof String ) {
			final String s = (String) value;
			try {
				if( decimalSeparator == DEFAULT_DECIMAL_SEPARATOR ) {
					result = new BigDecimal(s);
				} else {
					result = new BigDecimal(s.replace(decimalSeparator, DEFAULT_DECIMAL_SEPARATOR));
				}
			}
			catch(final NumberFormatException e) {
				throw new SuperCsvCellProcessorException(String.format("'%s' could not be parsed as a BigDecimal", value), context,
					this, e);
			}
		} else {
			throw new SuperCsvCellProcessorException(String.class, value, context, this);
		}
		
		return next.execute(result, context);
	}
}
