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

import java.util.Map;

import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Maps from one object to another, by looking up a <tt>Map</tt> with the input as the key, and returning its
 * corresponding value.
 * 
 * @since 1.50
 * @author Dominique De Vito
 * @author James Bassett
 */
public class HashMapper extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Map<Object, Object> mapping;
	private final Object defaultValue;
	
	/**
	 * Constructs a new <tt>HashMapper</tt> processor, which maps from one object to another, by looking up a
	 * <tt>Map</tt> with the input as the key, and returning its corresponding value. If no mapping is found, then
	 * <tt>null</tt> is returned.
	 * 
	 * @param mapping
	 *            the Map
	 * @throws NullPointerException
	 *             if mapping is null
	 * @throws IllegalArgumentException
	 *             if mapping is empty
	 */
	public HashMapper(final Map<Object, Object> mapping) {
		this(mapping, (Object) null);
	}
	
	/**
	 * Constructs a new <tt>HashMapper</tt> processor, which maps from one object to another, by looking up a
	 * <tt>Map</tt> with the input as the key, and returning its corresponding value. If no mapping is found, then the
	 * supplied default value is returned.
	 * 
	 * @param mapping
	 *            the Map
	 * @param defaultValue
	 *            the value to return if no mapping is found
	 * @throws NullPointerException
	 *             if mapping is null
	 * @throws IllegalArgumentException
	 *             if mapping is empty
	 */
	public HashMapper(final Map<Object, Object> mapping, final Object defaultValue) {
		super();
		checkPreconditions(mapping);
		this.mapping = mapping;
		this.defaultValue = defaultValue;
		
	}
	
	/**
	 * Constructs a new <tt>HashMapper</tt> processor, which maps from one object to another, by looking up a
	 * <tt>Map</tt> with the input as the key, and returning its corresponding value. If no mapping is found, then
	 * <tt>null</tt> is returned. Regardless of whether a mapping is found, the next processor in the chain will be
	 * called.
	 * 
	 * @param mapping
	 *            the Map
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if mapping or next is null
	 * @throws IllegalArgumentException
	 *             if mapping is empty
	 */
	public HashMapper(final Map<Object, Object> mapping, final BoolCellProcessor next) {
		this(mapping, null, next);
	}
	
	/**
	 * Constructs a new <tt>HashMapper</tt> processor, which maps from one object to another, by looking up a
	 * <tt>Map</tt> with the input as the key, and returning its corresponding value. If no mapping is found, then the
	 * supplied default value is returned. Regardless of whether a mapping is found, the next processor in the chain
	 * will be called.
	 * 
	 * @param mapping
	 *            the Map
	 * @param defaultValue
	 *            the value to return if no mapping is found
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if mapping or next is null
	 * @throws IllegalArgumentException
	 *             if mapping is empty
	 */
	public HashMapper(final Map<Object, Object> mapping, final Object defaultValue, final BoolCellProcessor next) {
		super(next);
		checkPreconditions(mapping);
		this.mapping = mapping;
		this.defaultValue = defaultValue;
	}
	
	/**
	 * Checks the preconditions for creating a new HashMapper processor.
	 * 
	 * @param mapping
	 *            the Map
	 * @throws NullPointerException
	 *             if mapping is null
	 * @throws IllegalArgumentException
	 *             if mapping is empty
	 */
	private static void checkPreconditions(final Map<Object, Object> mapping) {
		if( mapping == null ) {
			throw new NullPointerException("mapping should not be null");
		} else if( mapping.isEmpty() ) {
			throw new IllegalArgumentException("mapping should not be empty");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		Object result = mapping.get(value);
		if( result == null ) {
			result = defaultValue;
		}
		
		return next.execute(result, context);
	}
}
