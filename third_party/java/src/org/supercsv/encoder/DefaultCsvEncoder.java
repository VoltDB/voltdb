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
package org.supercsv.encoder;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * The default CsvEncoder implementation.
 * 
 * @author James Bassett
 * @since 2.1.0
 */
public class DefaultCsvEncoder implements CsvEncoder {
	
	private final StringBuilder currentColumn = new StringBuilder();
	
	/**
	 * Constructs a new <tt>DefaultCsvEncoder</tt>.
	 */
	public DefaultCsvEncoder() {
	}
	
	/**
	 * {@inheritDoc}
	 */
	public String encode(final String input, final CsvContext context, final CsvPreference preference) {
		
		currentColumn.delete(0, currentColumn.length()); // reusing builder object
		
		final int delimiter = preference.getDelimiterChar();
		final char quote = (char) preference.getQuoteChar();
		final String eolSymbols = preference.getEndOfLineSymbols();
		final int lastCharIndex = input.length() - 1;
		
		boolean quotesRequiredForSpecialChar = false;
		
		boolean skipNewline = false;
		
		for( int i = 0; i <= lastCharIndex; i++ ) {
			
			final char c = input.charAt(i);
			
			if( skipNewline ) {
				skipNewline = false;
				if( c == '\n' ) {
					continue; // newline following a carriage return is skipped
				}
			}
			
			if( c == delimiter ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(c);
			} else if( c == quote ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(quote);
				currentColumn.append(quote);
			} else if( c == '\r' ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
				context.setLineNumber(context.getLineNumber() + 1);
				skipNewline = true;
			} else if( c == '\n' ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
				context.setLineNumber(context.getLineNumber() + 1);
			} else {
				currentColumn.append(c);
			}
		}
		
		final boolean quotesRequiredForMode = preference.getQuoteMode().quotesRequired(input, context, preference);
		final boolean quotesRequiredForSurroundingSpaces = preference.isSurroundingSpacesNeedQuotes()
			&& input.length() > 0 && (input.charAt(0) == ' ' || input.charAt(input.length() - 1) == ' ');
		
		if( quotesRequiredForSpecialChar || quotesRequiredForMode || quotesRequiredForSurroundingSpaces ) {
			currentColumn.insert(0, quote).append(quote);
		}
		
		return currentColumn.toString();
	}
	
}
