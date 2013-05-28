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
 * This processor converts the input to a String, and ensures that the input's hash function matches any of a given set
 * of hashcodes. Lookup time is O(1).
 * <p>
 * This constraint is a very efficient way of ensuring constant expressions are present in certain columns of the CSV
 * file, such as "BOSS", "EMPLOYEE", or when a column denotes an action to be taken for the input line such as "D"
 * (delete), "I" (insert), ...
 * <p>
 * 
 * @since 1.50
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class RequireHashCode extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Set<Integer> requiredHashCodes = new HashSet<Integer>();
	
	/**
	 * Constructs a new <tt>RequireHashCode</tt> processor, which converts the input to a String, and ensures that the
	 * input's hash function matches any of a given set of hashcodes.
	 * 
	 * @param requiredHashcodes
	 *            one or more hashcodes
	 * @throws NullPointerException
	 *             if requiredHashcodes is null
	 * @throws IllegalArgumentException
	 *             if requiredHashcodes is empty
	 */
	public RequireHashCode(final int... requiredHashcodes) {
		super();
		checkPreconditions(requiredHashcodes);
		for( final int hash : requiredHashcodes ) {
			this.requiredHashCodes.add(hash);
		}
	}
	
	/**
	 * Constructs a new <tt>RequireHashCode</tt> processor, which converts the input to a String, ensures that the
	 * input's hash function matches the supplied hashcode, then calls the next processor in the chain.
	 * 
	 * @param requiredHashcode
	 *            the required hashcode
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public RequireHashCode(final int requiredHashcode, final CellProcessor next) {
		this(new int[] { requiredHashcode }, next);
	}
	
	/**
	 * Constructs a new <tt>RequireHashCode</tt> processor, which converts the input to a String, ensures that the
	 * input's hash function matches any of a given set of hashcodes, then calls the next processor in the chain.
	 * 
	 * @param requiredHashcodes
	 *            one or more hashcodes
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if requiredHashcodes or next is null
	 * @throws IllegalArgumentException
	 *             if requiredHashcodes is empty
	 */
	public RequireHashCode(final int[] requiredHashcodes, final CellProcessor next) {
		super(next);
		checkPreconditions(requiredHashcodes);
		for( final int hash : requiredHashcodes ) {
			this.requiredHashCodes.add(hash);
		}
	}
	
	/**
	 * Checks the preconditions for creating a new RequireHashCode processor.
	 * 
	 * @param requiredHashcodes
	 *            the supplied hashcodes
	 * @throws NullPointerException
	 *             if requiredHashcodes is null
	 * @throws IllegalArgumentException
	 *             if requiredHashcodes is empty
	 */
	private static void checkPreconditions(final int... requiredHashcodes) {
		if( requiredHashcodes == null ) {
			throw new NullPointerException("requiredHashcodes should not be null");
		} else if( requiredHashcodes.length == 0 ) {
			throw new IllegalArgumentException("requiredHashcodes should not be empty");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if value isn't one of the required hash codes
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		int hash = value.hashCode();
		if( !requiredHashCodes.contains(hash) ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"the hashcode of %d for value '%s' does not match any of the required hashcodes", hash, value),
				context, this);
		}
		
		return next.execute(value, context);
	}
	
}
