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

import java.util.Collection;

import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.util.CsvContext;

/**
 * This processor collects each value it encounters and adds it to the supplied Collection. You could supply a Set to
 * collect all of the unique values for a column, or a List to collect every value in a column in order. Just remember
 * that the larger your CSV file, the larger this Collection will be, so use with caution!
 * 
 * @since 2.1.0
 * @author James Bassett
 */
public class Collector extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Collection<Object> collection;
	
	/**
	 * Constructs a new <tt>Collector</tt>, which collects each value it encounters and adds it to the supplied
	 * Collection.
	 * 
	 * @param collection
	 *            the collection to add to
	 * @throws NullPointerException
	 *             if collection is null
	 */
	public Collector(final Collection<Object> collection) {
		super();
		checkPreconditions(collection);
		this.collection = collection;
	}
	
	/**
	 * Constructs a new <tt>Collector</tt>, which collects each value it encounters, adds it to the supplied Collection,
	 * then calls the next processor in the chain.
	 * 
	 * @param collection
	 *            the collection to add to
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if collection or next is null
	 */
	public Collector(final Collection<Object> collection, final CellProcessor next) {
		super(next);
		checkPreconditions(collection);
		this.collection = collection;
	}
	
	/**
	 * Checks the preconditions for creating a new Collector processor.
	 * 
	 * @param collection
	 *            the collection to add to
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
	 */
	public Object execute(final Object value, final CsvContext context) {
		collection.add(value);
		return next.execute(value, context);
	}
	
	/**
	 * Gets the collection of collected values.
	 * 
	 * @return the collection of collected values
	 */
	public Collection<Object> getCollection() {
		return collection;
	}
	
}
