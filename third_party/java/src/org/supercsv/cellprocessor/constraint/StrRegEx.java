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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * This constraint ensures that the input data matches the given regular expression.
 * 
 * @author Dominique De Vito
 * @author James Bassett
 * @since 1.50
 */
public class StrRegEx extends CellProcessorAdaptor implements StringCellProcessor {
	
	private final String regex;
	private final Pattern regexPattern;
	
	private static final Map<String, String> REGEX_MSGS = new HashMap<String, String>();
	
	/**
	 * Constructs a new <tt>StrRegEx</tt> processor, which ensures that the input data matches the given regular
	 * expression.
	 * 
	 * @param regex
	 *            the regular expression to match
	 * @throws NullPointerException
	 *             if regex is null
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws PatternSyntaxException
	 *             if regex is not a valid regular expression
	 */
	public StrRegEx(final String regex) {
		super();
		checkPreconditions(regex);
		this.regexPattern = Pattern.compile(regex);
		this.regex = regex;
	}
	
	/**
	 * Constructs a new <tt>StrRegEx</tt> processor, which ensures that the input data matches the given regular
	 * expression, then calls the next processor in the chain.
	 * 
	 * @param regex
	 *            the regular expression to match
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if regex is null
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws PatternSyntaxException
	 *             if regex is not a valid regular expression
	 */
	public StrRegEx(final String regex, final StringCellProcessor next) {
		super(next);
		checkPreconditions(regex);
		this.regexPattern = Pattern.compile(regex);
		this.regex = regex;
	}
	
	/**
	 * Checks the preconditions for creating a new StrRegEx processor.
	 * 
	 * @param regex
	 *            the regular expression to match
	 * @throws NullPointerException
	 *             if regex is null
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 */
	private static void checkPreconditions(final String regex) {
		if( regex == null ) {
			throw new NullPointerException("regex should not be null");
		} else if( regex.length() == 0 ) {
			throw new IllegalArgumentException("regex should not be empty");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if value doesn't match the regular expression
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final boolean matches = regexPattern.matcher((String) value).matches();
		if( !matches ) {
			final String msg = REGEX_MSGS.get(regex);
			if( msg == null ) {
				throw new SuperCsvConstraintViolationException(String.format(
					"'%s' does not match the regular expression '%s'", value, regex), context, this);
			} else {
				throw new SuperCsvConstraintViolationException(
					String.format("'%s' does not match the constraint '%s' defined by the regular expression '%s'",
						value, msg, regex), context, this);
			}
		}
		return next.execute(value, context);
	}
	
	/**
	 * Register a message detailing in plain language the constraint representing a regular expression. For example, the
	 * regular expression \d{0,6}(\.\d{0,3})? could be associated with the message
	 * "up to 6 digits whole digits, followed by up to 3 fractional digits".
	 * 
	 * @param regex
	 *            the regular expression
	 * @param message
	 *            the message to associate with the regex
	 */
	public static void registerMessage(String regex, String message) {
		REGEX_MSGS.put(regex, message);
	}
	
}
