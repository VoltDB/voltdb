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

import java.util.HashSet;
import java.util.Set;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * Ensure that upon processing a CSV file (reading or writing), that values of the column all are unique. Comparison is
 * based upon each elements <tt>equals()</tt> method of the objects and lookup takes O(1).
 * <P>
 * Compared to {@link UniqueHashCode} this processor potentially uses more memory, as it stores references to each
 * encountered object rather than just their hashcodes. On reading huge files this can be a real memory-hazard, however,
 * it ensures a true uniqueness check.
 * 
 * @since 1.50
 * @author Kasper B. Graversen
 * @author Dominique De Vito
 * @author James Bassett
 */
public class Unique extends CellProcessorAdaptor {
	
	private final Set<Object> encounteredElements = new HashSet<Object>();
	
	/**
	 * Constructs a new <tt>Unique</tt> processor, which ensures that all rows in a column are unique.
	 */
	public Unique() {
		super();
	}
	
	/**
	 * Constructs a new <tt>Unique</tt> processor, which ensures that all rows in a column are unique, then calls the
	 * next processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Unique(final CellProcessor next) {
		super(next);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a non-unique value is encountered
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( !encounteredElements.add(value) ) {
			throw new SuperCsvConstraintViolationException(String.format("duplicate value '%s' encountered", value), context, this);
		}
		
		return next.execute(value, context);
	}
}
