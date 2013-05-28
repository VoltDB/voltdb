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
 * This processor checks if the input is <tt>null</tt> or an empty string, and raises an exception in that case. In all
 * other cases, the next processor in the chain is invoked.
 * <p>
 * You should only use this processor, when a column must be non-null, but you do not need to apply any other processor
 * to the column.
 * <p>
 * If you apply other processors to the column, you can safely omit this processor as all other processors should do a
 * null-check on its input.
 * 
 * @since 1.50
 * @author Dominique De Vito
 */
public class StrNotNullOrEmpty extends CellProcessorAdaptor implements StringCellProcessor {
	
	/**
	 * Constructs a new <tt>StrNotNullOrEmpty</tt> processor, which checks for null/empty Strings.
	 */
	public StrNotNullOrEmpty() {
		super();
	}
	
	/**
	 * Constructs a new <tt>StrNotNullOrEmpty</tt> processor, which checks for null/empty Strings, then calls the next
	 * processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public StrNotNullOrEmpty(final CellProcessor next) {
		super(next);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null or isn't a String
	 * @throws SuperCsvConstraintViolationException
	 *             if value is an empty String
	 */
	public Object execute(final Object value, final CsvContext context) {
		if (value == null){
			throw new SuperCsvConstraintViolationException("the String should not be null", context, this);
		}
		
		if( value instanceof String ) {
			final String stringValue = (String) value;
			if( stringValue.length() == 0 ) {
				throw new SuperCsvConstraintViolationException("the String should not be empty", context, this);
			}
		} else {
			throw new SuperCsvCellProcessorException(String.class, value, context, this);
		}
		
		return next.execute(value, context);
	}
}
