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

import java.text.DecimalFormat;

import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Converts a double into a formatted string using the {@link DecimalFormat} class and the default locale. This is
 * useful, when you need to show numbers with a specific number of digits.
 * <p>
 * Please be aware that the constructors that use <tt>DecimalFormat</tt> are not thread-safe, so it is generally better
 * to use the constructors that accept a date format String.
 * <p>
 * In the format string, the following characters are defined as : <br>
 * 
 * <pre>
 * 0   - means Digit
 * #   - means Digit, zero shows as absent (works only as zero padding on the right hand side of the number)
 * .   - means Decimal separator or monetary decimal separator
 * -   - means Minus sign
 * ,   - means Grouping separator
 * </pre>
 * 
 * <br>
 * If you want to convert from a String to a decimal, use the {@link ParseDouble} or {@link ParseBigDecimal} processor.
 * 
 * @since 1.50
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class FmtNumber extends CellProcessorAdaptor implements DoubleCellProcessor, LongCellProcessor {
	
	/** the decimal format string */
	private final String decimalFormat;
	
	/** the decimal format object - not thread safe */
	private final DecimalFormat formatter;
	
	/**
	 * Constructs a new <tt>FmtNumber</tt> processor, which converts a double into a formatted string using the supplied
	 * decimal format String. This constructor is thread-safe.
	 * 
	 * @param decimalFormat
	 *            the decimal format String (see {@link DecimalFormat})
	 * @throws NullPointerException
	 *             if decimalFormat is null
	 */
	public FmtNumber(final String decimalFormat) {
		super();
		checkPreconditions(decimalFormat);
		this.decimalFormat = decimalFormat;
		this.formatter = null;
	}
	
	/**
	 * Constructs a new <tt>FmtNumber</tt> processor, which converts a double into a formatted string using the supplied
	 * decimal format String, then calls the next processor in the chain. This constructor is thread-safe.
	 * 
	 * @param decimalFormat
	 *            the decimal format String (see {@link DecimalFormat})
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if decimalFormat or next is null
	 */
	public FmtNumber(final String decimalFormat, final StringCellProcessor next) {
		super(next);
		checkPreconditions(decimalFormat);
		this.decimalFormat = decimalFormat;
		this.formatter = null;
	}
	
	/**
	 * Constructs a new <tt>FmtNumber</tt> processor, which converts a double into a formatted string using the supplied
	 * decimal format. This constructor is not thread-safe.
	 * 
	 * @param formatter
	 *            the DecimalFormat
	 * @throws NullPointerException
	 *             if formatter is null
	 */
	public FmtNumber(final DecimalFormat formatter) {
		super();
		checkPreconditions(formatter);
		this.formatter = formatter;
		this.decimalFormat = null;
	}
	
	/**
	 * Constructs a new <tt>FmtNumber</tt> processor, which converts a double into a formatted string using the supplied
	 * decimal format, then calls the next processor in the chain. This constructor is not thread-safe.
	 * 
	 * @param formatter
	 *            the DecimalFormat
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if formatter or next is null
	 */
	public FmtNumber(final DecimalFormat formatter, final StringCellProcessor next) {
		super(next);
		checkPreconditions(formatter);
		this.formatter = formatter;
		this.decimalFormat = null;
	}
	
	/**
	 * Checks the preconditions for creating a new FmtNumber processor with a date format String.
	 * 
	 * @param dateFormat
	 *            the date format String
	 * @throws NullPointerException
	 *             if dateFormat is null
	 */
	private static void checkPreconditions(final String dateFormat) {
		if( dateFormat == null ) {
			throw new NullPointerException("dateFormat should not be null");
		}
	}
	
	/**
	 * Checks the preconditions for creating a new FmtNumber processor with a DecimalFormat.
	 * 
	 * @param formatter
	 *            the DecimalFormat
	 * @throws NullPointerException
	 *             if formatter is null
	 */
	private static void checkPreconditions(final DecimalFormat formatter) {
		if( formatter == null ) {
			throw new NullPointerException("formatter should not be null");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null or not a Number, or if an invalid decimalFormat String was supplied
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( !(value instanceof Number) ) {
			throw new SuperCsvCellProcessorException(Number.class, value, context, this);
		}
		
		// create a new DecimalFormat if one is not supplied
		final DecimalFormat decimalFormatter;
		try {
			decimalFormatter = formatter != null ? formatter : new DecimalFormat(decimalFormat);
		}
		catch(IllegalArgumentException e) {
			throw new SuperCsvCellProcessorException(
				String.format("'%s' is not a valid decimal format", decimalFormat), context, this, e);
		}
		
		final String result = decimalFormatter.format(value);
		return next.execute(result, context);
	}
}
