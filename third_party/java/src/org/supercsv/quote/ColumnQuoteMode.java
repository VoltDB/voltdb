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
package org.supercsv.quote;

import java.util.HashSet;
import java.util.Set;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * When using ColumnQuoteMode surrounding quotes are only applied if required to escape special characters (per
 * RFC4180), or if a particular column should always be quoted.
 * 
 * @author James Bassett
 * @since 2.1.0
 */
public class ColumnQuoteMode implements QuoteMode {
	
	private final Set<Integer> columnNumbers = new HashSet<Integer>();
	
	/**
	 * Constructs a new <tt>ColumnQuoteMode</tt> that quotes columns by column number. If no column numbers are supplied
	 * (i.e. no parameters) then quoting will be determined per RFC4180.
	 * 
	 * @param columnsToQuote
	 *            the column numbers to quote
	 */
	public ColumnQuoteMode(final int... columnsToQuote) {
		if( columnsToQuote == null ) {
			throw new NullPointerException("columnsToQuote should not be null");
		}
		for( final Integer columnToQuote : columnsToQuote ) {
			columnNumbers.add(columnToQuote);
		}
	}
	
	/**
	 * Constructs a new <tt>ColumnQuoteMode</tt> that quotes columns if the element representing that column in the
	 * supplied array is true. Please note that <tt>false</tt> doesn't disable quoting, it just means that quoting is
	 * determined by RFC4180.
	 * 
	 * @param columnsToQuote
	 *            array of booleans (one per CSV column) indicating whether each column should be quoted or not
	 * @throws NullPointerException
	 *             if columnsToQuote is null
	 */
	public ColumnQuoteMode(final boolean[] columnsToQuote) {
		if( columnsToQuote == null ) {
			throw new NullPointerException("columnsToQuote should not be null");
		}
		for( int i = 0; i < columnsToQuote.length; i++ ) {
			if( columnsToQuote[i] ) {
				columnNumbers.add(i + 1); // column numbers start at 1
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean quotesRequired(final String csvColumn, final CsvContext context, final CsvPreference preference) {
		return columnNumbers.contains(context.getColumnNumber());
	}
	
}
