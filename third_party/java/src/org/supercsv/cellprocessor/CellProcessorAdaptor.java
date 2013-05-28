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
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Abstract super class containing shared behaviour of all cell processors. Processors are linked together in a linked
 * list. The end element of this list should always be an instance of <tt>NullObjectPattern</tt>.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public abstract class CellProcessorAdaptor implements CellProcessor {
	
	/** the next processor in the chain */
	protected final CellProcessor next;
	
	/**
	 * Constructor used by CellProcessors to indicate that they are the last processor in the chain.
	 */
	protected CellProcessorAdaptor() {
		super();
		this.next = NullObjectPattern.INSTANCE;
	}
	
	/**
	 * Constructor used by CellProcessors that require <tt>CellProcessor</tt> chaining (further processing is required).
	 * 
	 * @param next
	 *            the next <tt>CellProcessor</tt> in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	protected CellProcessorAdaptor(final CellProcessor next) {
		super();
		if( next == null ) {
			throw new NullPointerException("next CellProcessor should not be null");
		}
		this.next = next;
	}
	
	/**
	 * Checks that the input value is not <tt>null</tt>, throwing a <tt>NullInputException</tt> if it is. This method
	 * should be called by all processors that need to ensure the input is not <tt>null</tt>.
	 * 
	 * @param value
	 *            the input value
	 * @param context
	 *            the CSV context
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @since 2.0.0
	 */
	protected final void validateInputNotNull(final Object value, final CsvContext context) {
		if( value == null ) {
			throw new SuperCsvCellProcessorException(
				"this processor does not accept null input - if the column is optional then chain an Optional() processor before this one",
				context, this);
		}
	}
	
	/**
	 * Returns the CellProccessor's fully qualified class name.
	 */
	@Override
	public String toString() {
		return getClass().getName();
	}
	
	/**
	 * This is an implementation-specific processor and should only be used by the <tt>CellProcessorAdaptor</tt> class.
	 * It is the implementation of the null object pattern (it does nothing - just returns the value!) and should always
	 * be the last <tt>CellProcessor</tt> in the chain. It is implemented as a reusable singleton to avoid unnecessary
	 * object creation.
	 * 
	 * @author Kasper B. Graversen
	 * @author James Bassett
	 */
	private static final class NullObjectPattern implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
		LongCellProcessor, StringCellProcessor {
		
		private static final NullObjectPattern INSTANCE = new NullObjectPattern();
		
		/*
		 * This processor must not be instantiated outside of CellProcessorAdaptor.
		 */
		private NullObjectPattern() {
			super();
		}
		
		/**
		 * {@inheritDoc}
		 */
		public Object execute(final Object value, final CsvContext context) {
			return value;
		}
	}
	
}
