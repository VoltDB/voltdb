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

import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Converts a Boolean into a formatted string. If you want to convert from a String to a Boolean, use the
 * {@link ParseBool} processor.
 * 
 * @since 1.50
 * @author Dominique De Vito
 */
public class FmtBool extends CellProcessorAdaptor implements BoolCellProcessor {
	
	private final String trueValue;
	private final String falseValue;
	
	/**
	 * Constructs a new <tt>FmtBool</tt> processor, which converts a Boolean into a formatted string.
	 * 
	 * @param trueValue
	 *            the String to use if the value is true
	 * @param falseValue
	 *            the String to use if the value is false
	 */
	public FmtBool(final String trueValue, final String falseValue) {
		super();
		this.trueValue = trueValue;
		this.falseValue = falseValue;
	}
	
	/**
	 * Constructs a new <tt>FmtBool</tt> processor, which converts a Boolean into a formatted string, then calls the
	 * next processor in the chain.
	 * 
	 * @param trueValue
	 *            the String to use if the value is true
	 * @param falseValue
	 *            the String to use if the value is false
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public FmtBool(final String trueValue, final String falseValue, final StringCellProcessor next) {
		super(next);
		this.trueValue = trueValue;
		this.falseValue = falseValue;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null or is not a Boolean
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( !(value instanceof Boolean) ) {
			throw new SuperCsvCellProcessorException(Boolean.class, value, context, this);
		}
		
		final String result = ((Boolean) value).booleanValue() ? trueValue : falseValue;
		return next.execute(result, context);
	}
}
