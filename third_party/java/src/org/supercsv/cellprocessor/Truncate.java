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
 * Ensure that Strings or String-representations of objects are truncated to a maximum size. If you desire, you can
 * append a String to denote that the data has been truncated (e.g. "...").
 * <p>
 * As of 2.0.0, this functionality was moved from the {@link Trim} processor to this processor, to allow a clear
 * distinction between trimming and truncating.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class Truncate extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private static final String EMPTY_STRING = "";
	
	private final int maxSize;
	private final String suffix;
	
	/**
	 * Constructs a new <tt>Truncate</tt> processor, which truncates a String to ensure it is no longer than the
	 * specified size.
	 * 
	 * @param maxSize
	 *            the maximum size of the String
	 * @throws IllegalArgumentException
	 *             if maxSize <= 0
	 */
	public Truncate(final int maxSize) {
		this(maxSize, EMPTY_STRING);
	}
	
	/**
	 * Constructs a new <tt>Truncate</tt> processor, which truncates a String to ensure it is no longer than the
	 * specified size, then appends the <code>suffix</code> String to indicate that the String has been truncated.
	 * 
	 * @param maxSize
	 *            the maximum size of the String
	 * @param suffix
	 *            the String to append if the input is truncated (e.g. "...")
	 * @throws IllegalArgumentException
	 *             if maxSize <= 0
	 * @throws NullPointerException
	 *             if suffix is null
	 */
	public Truncate(final int maxSize, final String suffix) {
		checkPreconditions(maxSize, suffix);
		this.maxSize = maxSize;
		this.suffix = suffix;
	}
	
	/**
	 * Constructs a new <tt>Truncate</tt> processor, which truncates a String to ensure it is no longer than the
	 * specified size, then appends the <code>suffix</code> String to indicate that the String has been truncated and
	 * calls the next processor in the chain.
	 * 
	 * @param maxSize
	 *            the maximum size of the String
	 * @param suffix
	 *            the String to append if the input is truncated (e.g. "...")
	 * @param next
	 *            the next processor in the chain
	 * @throws IllegalArgumentException
	 *             if maxSize <= 0
	 * @throws NullPointerException
	 *             if suffix or next is null
	 */
	public Truncate(final int maxSize, final String suffix, final StringCellProcessor next) {
		super(next);
		checkPreconditions(maxSize, suffix);
		this.maxSize = maxSize;
		this.suffix = suffix;
	}
	
	/**
	 * Constructs a new <tt>Truncate</tt> processor, which truncates a String to ensure it is no longer than the
	 * specified size, then calls the next processor in the chain.
	 * 
	 * @param maxSize
	 *            the maximum size of the String
	 * @param next
	 *            the next processor in the chain
	 * @throws IllegalArgumentException
	 *             if maxSize <= 0
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Truncate(final int maxSize, final StringCellProcessor next) {
		this(maxSize, EMPTY_STRING, next);
	}
	
	/**
	 * Checks the preconditions for creating a new Truncate processor.
	 * 
	 * @param maxSize
	 *            the maximum size of the String
	 * @param suffix
	 *            the String to append if the input is truncated (e.g. "...")
	 * @throws IllegalArgumentException
	 *             if maxSize <= 0
	 * @throws NullPointerException
	 *             if suffix is null
	 */
	private static void checkPreconditions(final int maxSize, final String suffix) {
		if( maxSize <= 0 ) {
			throw new IllegalArgumentException(String.format("maxSize should be > 0 but was %d", maxSize));
		}
		if( suffix == null ) {
			throw new NullPointerException("suffix should not be null");
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
		
		final String stringValue = value.toString();
		final String result;
		if( stringValue.length() <= maxSize ) {
			result = stringValue;
		} else {
			result = stringValue.substring(0, maxSize) + suffix;
		}
		
		return next.execute(result, context);
	}
}
