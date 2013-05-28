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
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Ensure that Strings or String-representations of objects are trimmed (contain no surrounding whitespace).
 * <p>
 * Prior to 2.0.0, this processor truncated Strings - this functionality can now be found in the {@link Truncate}
 * processor.
 * 
 * @author James Bassett
 */
public class Trim extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
	LongCellProcessor, StringCellProcessor {
	
	/**
	 * Constructs a new <tt>Trim</tt> processor, which trims a String to ensure it has no surrounding whitespace.
	 */
	public Trim() {
		super();
	}
	
	/**
	 * Constructs a new <tt>Trim</tt> processor, which trims a String to ensure it has no surrounding whitespace then
	 * calls the next processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Trim(final StringCellProcessor next) {
		super(next);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final String result = value.toString().trim();
		return next.execute(result, context);
	}
}
