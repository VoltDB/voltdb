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

import java.util.Collection;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * This processor ensures that the input value is an element of a Collection. It differs from {@link IsIncludedIn} as
 * the Collection is modifiable, so it can be used with other processors such as
 * {@link org.supercsv.cellprocessor.Collector Collector} to enforce referential integrity.
 * 
 * @since 2.1.0
 * @author James Bassett
 */
public class IsElementOf extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Collection<Object> collection;
	
	/**
	 * Constructs a new <tt>IsElementOf</tt>, which ensures that the input value is an element of a Collection.
	 * 
	 * @param collection
	 *            the collection to check
	 * @throws NullPointerException
	 *             if collection is null
	 */
	public IsElementOf(final Collection<Object> collection) {
		super();
		checkPreconditions(collection);
		this.collection = collection;
	}
	
	/**
	 * Constructs a new <tt>IsElementOf</tt>, which ensures that the input value is an element of a Collection, then
	 * calls the next processor in the chain.
	 * 
	 * @param collection
	 *            the collection to check
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if collection or next is null
	 */
	public IsElementOf(final Collection<Object> collection, final CellProcessor next) {
		super(next);
		checkPreconditions(collection);
		this.collection = collection;
	}
	
	/**
	 * Checks the preconditions for creating a new IsElementOf processor.
	 * 
	 * @param collection
	 *            the collection to check
	 * @throws NullPointerException
	 *             if collection is null
	 */
	private static void checkPreconditions(final Collection<Object> collection) {
		if( collection == null ) {
			throw new NullPointerException("collection should not be null");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvConstraintViolationException
	 *             if value isn't in the Collection
	 */
	public Object execute(final Object value, final CsvContext context) {
		
		if( !collection.contains(value) ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"'%s' is not an element of the supplied Collection", value), context, this);
		}
		
		return next.execute(value, context);
	}
	
}
