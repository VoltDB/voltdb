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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.supercsv.cellprocessor.ift.BoolCellProcessor;
import org.supercsv.cellprocessor.ift.DateCellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

/**
 * Replaces each substring of the input string that matches the given regular expression with the given replacement. The
 * regular expression pattern is compiled once then reused for efficiency.
 * 
 * @author Kasper B. Graversen
 * @author Dominique De Vito
 * @author James Bassett
 */
public class StrReplace extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor,
	DoubleCellProcessor, LongCellProcessor, StringCellProcessor {
	
	private final Pattern regexPattern;
	private final String replacement;
	
	/**
	 * Constructs a new <tt>StrReplace</tt> processor, which replaces each substring of the input that matches the regex
	 * with the supplied replacement.
	 * 
	 * @param regex
	 *            the regular expression to match
	 * @param replacement
	 *            the string to be substituted for each match
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws NullPointerException
	 *             if regex or replacement is null
	 * @throws PatternSyntaxException
	 *             if regex is not a valid regular expression
	 */
	public StrReplace(final String regex, final String replacement) {
		super();
		checkPreconditions(regex, replacement);
		this.regexPattern = Pattern.compile(regex);
		this.replacement = replacement;
	}
	
	/**
	 * Constructs a new <tt>StrReplace</tt> processor, which replaces each substring of the input that matches the regex
	 * with the supplied replacement, then calls the next processor in the chain.
	 * 
	 * @param regex
	 *            the regular expression to match
	 * @param replacement
	 *            the string to be substituted for each match
	 * @param next
	 *            the next processor in the chain
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws NullPointerException
	 *             if regex or replacement is null
	 * @throws PatternSyntaxException
	 *             if regex is not a valid regular expression
	 */
	public StrReplace(final String regex, final String replacement, final StringCellProcessor next) {
		super(next);
		checkPreconditions(regex, replacement);
		this.regexPattern = Pattern.compile(regex);
		this.replacement = replacement;
	}
	
	/**
	 * Checks the preconditions for creating a new StrRegExReplace processor.
	 * 
	 * @param regex
	 *            the supplied regular expression
	 * @param replacement
	 *            the supplied replacement text
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws NullPointerException
	 *             if regex or replacement is null
	 */
	private static void checkPreconditions(final String regex, final String replacement) {
		if( regex == null ) {
			throw new NullPointerException("regex should not be null");
		} else if( regex.length() == 0 ) {
			throw new IllegalArgumentException("regex should not be empty");
		}
		
		if( replacement == null ) {
			throw new NullPointerException("replacement should not be null");
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
		String result = regexPattern.matcher(value.toString()).replaceAll(replacement);
		return next.execute(result, context);
	}
	
}
