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
import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * This processor ensures that the input is not <tt>null</tt>.
 * <p>
 * You should only use this processor when a column must be non-null, but you do not need to apply any other processor
 * to the column (i.e. a mandatory String column with no other conversions or constraints)
 * <p>
 * If you apply other processors to the column, you can safely omit this processor as all other processors should do a
 * null-check on its input.
 * 
 * @since 1.50
 * @author Dominique De Vito
 */
public class NotNull extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
	LongCellProcessor, StringCellProcessor {
	
	/**
	 * Constructs a new <tt>NotNull</tt> which ensures that the input is not <tt>null</tt>.
	 */
	public NotNull() {
		super();
	}
	
	/**
	 * Constructs a new <tt>NotNull</tt> which ensures that the input is not <tt>null</tt>, then calls the next
	 * processor in the chain. All other processor should check for <tt>null</tt> inputs, so this constructor is not
	 * typically required.
	 * 
	 * @param next
	 *            the next processor in the chain
	 */
	public NotNull(final CellProcessor next) {
		super(next);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 */
	public Object execute(final Object value, final CsvContext context) {
		if (value == null){
			throw new SuperCsvConstraintViolationException("null value encountered", context, this);
		}
		return next.execute(value, context);
	}
}
