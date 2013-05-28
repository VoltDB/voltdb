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

import java.util.Collections;
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
 * This processor ensures that the input value belongs to a specific set of (unchangeable) values. If you want to check
 * if the value is an element of a (possibly changing) Collection, then use {@link IsElementOf} instead.
 * 
 * @since 1.50
 * @author Dominique De Vito
 * @author James Bassett
 */
public class IsIncludedIn extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Set<Object> possibleValues = new HashSet<Object>();
	
	/**
	 * Constructs a new <tt>IsIncludedIn</tt> processor, which ensures that the input value belongs to a specific set of
	 * given values.
	 * 
	 * @param possibleValues
	 *            the Set of values
	 * @throws NullPointerException
	 *             if possibleValues is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	public IsIncludedIn(final Set<Object> possibleValues) {
		super();
		checkPreconditions(possibleValues);
		this.possibleValues.addAll(possibleValues);
	}
	
	/**
	 * Constructs a new <tt>IsIncludedIn</tt> processor, which ensures that the input value belongs to a specific set of
	 * given values, then calls the next processor in the chain.
	 * 
	 * @param possibleValues
	 *            the Set of values
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if possibleValues or next is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	public IsIncludedIn(final Set<Object> possibleValues, final CellProcessor next) {
		super(next);
		checkPreconditions(possibleValues);
		this.possibleValues.addAll(possibleValues);
	}
	
	/**
	 * Constructs a new <tt>IsIncludedIn</tt> processor, which ensures that the input value belongs to a specific set of
	 * given values.
	 * 
	 * @param possibleValues
	 *            the array of values
	 * @throws NullPointerException
	 *             if possibleValues is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	public IsIncludedIn(final Object[] possibleValues) {
		super();
		checkPreconditions(possibleValues);
		Collections.addAll(this.possibleValues, possibleValues);
	}
	
	/**
	 * Constructs a new <tt>IsIncludedIn</tt> processor, which ensures that the input value belongs to a specific set of
	 * given values, then calls the next processor in the chain.
	 * 
	 * @param possibleValues
	 *            the array of values
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if possibleValues or next is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	public IsIncludedIn(final Object[] possibleValues, final CellProcessor next) {
		super(next);
		checkPreconditions(possibleValues);
		Collections.addAll(this.possibleValues, possibleValues);
	}
	
	/**
	 * Checks the preconditions for creating a new IsIncludedIn processor with a Set of Objects.
	 * 
	 * @param possibleValues
	 *            the Set of possible values
	 * @throws NullPointerException
	 *             if possibleValues is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	private static void checkPreconditions(final Set<Object> possibleValues) {
		if( possibleValues == null ) {
			throw new NullPointerException("possibleValues Set should not be null");
		} else if( possibleValues.isEmpty() ) {
			throw new IllegalArgumentException("possibleValues Set should not be empty");
		}
	}
	
	/**
	 * Checks the preconditions for creating a new IsIncludedIn processor with a array of Objects.
	 * 
	 * @param possibleValues
	 *            the array of possible values
	 * @throws NullPointerException
	 *             if possibleValues is null
	 * @throws IllegalArgumentException
	 *             if possibleValues is empty
	 */
	private static void checkPreconditions(final Object... possibleValues) {
		if( possibleValues == null ) {
			throw new NullPointerException("possibleValues array should not be null");
		} else if( possibleValues.length == 0 ) {
			throw new IllegalArgumentException("possibleValues array should not be empty");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if value isn't one of the possible values
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( !possibleValues.contains(value) ) {
			throw new SuperCsvConstraintViolationException(String.format(
				"'%s' is not included in the allowed set of values", value), context, this);
		}
		
		return next.execute(value, context);
	}
}
