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
 * This processor is used in the situations you want to be able to check for the presence of a "special
 * token". Such a token could be the string "[empty]" which could denote that a column is different from the empty
 * string "".
 * <p>
 * For example, to convert the String <tt>"[empty]"</tt> to -1 (an int representing 'empty') you could use <code>
 * new Token("[empty]", -1)
 * </code>
 * <p>
 * Comparison between the input and the <tt>token</tt> is based on the object's <tt>equals()</tt> method.
 * 
 * @since 1.02
 * @author Kasper B. Graversen
 */
public class Token extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
	LongCellProcessor, StringCellProcessor {
	
	private final Object returnValue;
	private final Object token;
	
	/**
	 * Constructs a new <tt>Token</tt> processor, which returns the supplied value if the token is encountered,
	 * otherwise it returns the input unchanged.
	 * 
	 * @param token
	 *            the token
	 * @param returnValue
	 *            the value to return if the token is encountered
	 */
	public Token(final Object token, final Object returnValue) {
		super();
		this.token = token;
		this.returnValue = returnValue;
	}
	
	/**
	 * Constructs a new <tt>Token</tt> processor, which returns the supplied value if the token is encountered,
	 * otherwise it passes the input unchanged to the next processor in the chain.
	 * 
	 * @param token
	 *            the token
	 * @param returnValue
	 *            the value to return if the token is encountered
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Token(final Object token, final Object returnValue, final CellProcessor next) {
		super(next);
		this.token = token;
		this.returnValue = returnValue;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		if( value.equals(token) ) {
			return returnValue;
		}
		
		return next.execute(value, context);
	}
}
