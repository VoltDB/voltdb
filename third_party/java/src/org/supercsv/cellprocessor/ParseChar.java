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

import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Converts a String to a Character. If the String has a length > 1, then an Exception is thrown.
 * 
 * @since 1.10
 * @author Kasper B. Graversen
 */
public class ParseChar extends CellProcessorAdaptor implements StringCellProcessor {
	
	/**
	 * Constructs a new <tt>ParseChar</tt> processor, which converts a String to a Character.
	 */
	public ParseChar() {
		super();
	}
	
	/**
	 * Constructs a new <tt>ParseChar</tt> processor, which converts a String to a Character, then calls the next
	 * processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public ParseChar(final DoubleCellProcessor next) {
		super(next);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null, isn't a Character or String, or is a String of multiple characters
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final Character result;
		if( value instanceof Character ) {
			result = (Character) value;
		} else if( value instanceof String ) {
			final String stringValue = (String) value;
			if( stringValue.length() == 1 ) {
				result = Character.valueOf(stringValue.charAt(0));
			} else {
				throw new SuperCsvCellProcessorException(String.format(
					"'%s' cannot be parsed as a char as it is a String longer than 1 character", stringValue), context,
					this);
			}
		} else {
			final String actualClassName = value.getClass().getName();
			throw new SuperCsvCellProcessorException(String.format(
				"the input value should be of type Character or String but is of type %s", actualClassName), context,
				this);
		}
		
		return next.execute(result, context);
	}
}
