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
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * This constraint ensures that all input data is equal (to each other, or to a supplied constant value).
 * 
 * @author Dominique De Vito
 * @author James Bassett
 * @since 1.50
 */
public class Equals extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
	LongCellProcessor, StringCellProcessor {
	
	private static final Object UNKNOWN = new Object();
	
	private Object constantValue;
	private boolean constantSupplied;
	
	/**
	 * Constructs a new <tt>Equals</tt> processor, which ensures all input data is equal.
	 */
	public Equals() {
		super();
		this.constantValue = UNKNOWN;
		this.constantSupplied = false;
	}
	
	/**
	 * Constructs a new <tt>Equals</tt> processor, which ensures all input data is equal to the supplied constant value.
	 * 
	 * @param constantValue
	 *            the constant value that all input must equal
	 */
	public Equals(Object constantValue) {
		super();
		this.constantValue = constantValue;
		this.constantSupplied = true;
	}
	
	/**
	 * Constructs a new <tt>Equals</tt> processor, which ensures all input data is equal, then calls the the next
	 * processor in the chain.
	 * 
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Equals(final CellProcessor next) {
		super(next);
		this.constantValue = UNKNOWN;
		this.constantSupplied = false;
	}
	
	/**
	 * Constructs a new <tt>Equals</tt> processor, which ensures all input data is equal to the supplied constant value,
	 * then calls the the next processor in the chain.
	 * 
	 * @param constantValue
	 *            the constant value that all input must equal
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Equals(final Object constantValue, final CellProcessor next) {
		super(next);
		this.constantValue = constantValue;
		this.constantSupplied = true;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvConstraintViolationException
	 *             if value isn't equal to the constant value (or previously encountered value if a constant wasn't
	 *             supplied)
	 */
	public Object execute(final Object value, final CsvContext context) {
		if( UNKNOWN.equals(constantValue) ) {
			constantValue = value; // no constant supplied, so remember the first value encountered
		} else {
			if( !equals(constantValue, value) ) {
				if( constantSupplied ) {
					throw new SuperCsvConstraintViolationException(String.format("'%s' is not equal to the supplied constant '%s'", value,
						constantValue), context, this);
				} else {
					throw new SuperCsvConstraintViolationException(String.format("'%s' is not equal to the previous value(s) of '%s'",
						value, constantValue), context, this);
				}
			}
		}
		return next.execute(value, context);
	}
	
	/**
	 * Returns true if both objects are null or equal, otherwise false.
	 * 
	 * @param o1
	 *            the first object
	 * @param o2
	 *            the second object
	 * @return true if both objects are null or equal, otherwise false
	 */
	private static boolean equals(Object o1, Object o2) {
		return (o1 == null) ? (o2 == null) : o1.equals(o2);
	}
	
}
