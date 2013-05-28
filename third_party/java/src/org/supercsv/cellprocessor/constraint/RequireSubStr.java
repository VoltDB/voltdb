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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.util.CsvContext;

/**
 * Converts the input to a String and ensures that the input contains at least one of the specified substrings.
 * 
 * @since 1.10
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class RequireSubStr extends CellProcessorAdaptor implements StringCellProcessor {
	
	private final List<String> requiredSubStrings = new ArrayList<String>();
	
	/**
	 * Converts the input to a String and ensures that the input contains at least one of the specified substrings.
	 * 
	 * @param requiredSubStrings
	 *            the required substrings
	 * @throws NullPointerException
	 *             if requiredSubStrings or one of its elements is null
	 * @throws IllegalArgumentException
	 *             if requiredSubStrings is empty
	 */
	public RequireSubStr(final String... requiredSubStrings) {
		super();
		checkPreconditions(requiredSubStrings);
		checkAndAddRequiredSubStrings(requiredSubStrings);
	}
	
	/**
	 * Converts the input to a String, ensures that the input contains at least one of the specified substrings, then
	 * calls the next processor in the chain.
	 * 
	 * @param requiredSubStrings
	 *            the List of required substrings
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if requiredSubStrings, one of its elements or next is null
	 * @throws IllegalArgumentException
	 *             if requiredSubStrings is empty
	 */
	public RequireSubStr(final List<String> requiredSubStrings, final CellProcessor next) {
		super(next);
		checkPreconditions(requiredSubStrings);
		checkAndAddRequiredSubStrings(requiredSubStrings);
	}
	
	/**
	 * Converts the input to a String, ensures that the input contains the specified substring, then calls the next
	 * processor in the chain.
	 * 
	 * @param requiredSubString
	 *            the required substring
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if requiredSubString or next is null
	 */
	public RequireSubStr(final String requiredSubString, final CellProcessor next) {
		super(next);
		checkPreconditions(requiredSubString);
		checkAndAddRequiredSubStrings(requiredSubString);
	}
	
	/**
	 * Converts the input to a String, ensures that the input contains at least one of the specified substrings, then
	 * calls the next processor in the chain.
	 * 
	 * @param requiredSubStrings
	 *            the List of required substrings
	 * @param next
	 *            the next processor in the chain
	 * @throws NullPointerException
	 *             if requiredSubStrings, one of its elements or next is null
	 * @throws IllegalArgumentException
	 *             if requiredSubStrings is empty
	 */
	public RequireSubStr(final String[] requiredSubStrings, final CellProcessor next) {
		super(next);
		checkPreconditions(requiredSubStrings);
		checkAndAddRequiredSubStrings(requiredSubStrings);
	}
	
	/**
	 * Checks the preconditions for creating a new RequireSubStr processor with an array of Strings.
	 * 
	 * @param requiredSubStrings
	 *            the required substrings
	 * @throws NullPointerException
	 *             if requiredSubStrings or one of its elements is null
	 * @throws IllegalArgumentException
	 *             if requiredSubStrings is empty
	 */
	private static void checkPreconditions(String... requiredSubStrings) {
		if( requiredSubStrings == null ) {
			throw new NullPointerException("requiredSubStrings array should not be null");
		} else if( requiredSubStrings.length == 0 ) {
			throw new IllegalArgumentException("requiredSubStrings array should not be empty");
		}
	}
	
	/**
	 * Checks the preconditions for creating a new RequireSubStr processor with a List of Strings.
	 * 
	 * @param requiredSubStrings
	 *            the required substrings
	 * @throws NullPointerException
	 *             if requiredSubStrings or one of its elements is null
	 * @throws IllegalArgumentException
	 *             if requiredSubStrings is empty
	 */
	private static void checkPreconditions(List<String> requiredSubStrings) {
		if( requiredSubStrings == null ) {
			throw new NullPointerException("requiredSubStrings List should not be null");
		} else if( requiredSubStrings.isEmpty() ) {
			throw new IllegalArgumentException("requiredSubStrings List should not be empty");
		}
	}
	
	/**
	 * Adds each required substring, checking that it's not null.
	 * 
	 * @param requiredSubStrings
	 *            the required substrings
	 * @throws NullPointerException
	 *             if a required substring is null
	 */
	private void checkAndAddRequiredSubStrings(final List<String> requiredSubStrings) {
		for( String required : requiredSubStrings ) {
			if( required == null ) {
				throw new NullPointerException("required substring should not be null");
			}
			this.requiredSubStrings.add(required);
		}
	}
	
	/**
	 * Adds each required substring, checking that it's not null.
	 * 
	 * @param requiredSubStrings
	 *            the required substrings
	 * @throws NullPointerException
	 *             if a required substring is null
	 */
	private void checkAndAddRequiredSubStrings(final String... requiredSubStrings) {
		checkAndAddRequiredSubStrings(Arrays.asList(requiredSubStrings));
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws SuperCsvCellProcessorException
	 *             if value is null
	 * @throws SuperCsvConstraintViolationException
	 *             if value doesn't contain any of the required substrings
	 */
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context);
		
		final String stringValue = value.toString();
		
		for( final String required : requiredSubStrings ) {
			if( stringValue.contains(required) ) {
				return next.execute(value, context); // just need to match a single substring
			}
		}
		
		throw new SuperCsvConstraintViolationException(String.format("'%s' does not contain any of the required substrings", value),
			context, this);
	}
}
