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

import java.util.HashSet;
import java.util.Set;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * A selective CsvEncoder implementation - only the desired column numbers (if any) are encoded. Use with caution -
 * disabling encoding may increase performance but at the risk of invalid CSV.
 * 
 * @author James Bassett
 * @since 2.1.0
 */
public class SelectiveCsvEncoder extends DefaultCsvEncoder {
	
	private final Set<Integer> columnNumbers = new HashSet<Integer>();
	
	/**
	 * Constructs a new <tt>SelectiveCsvEncoder</tt> that encodes columns by column number. If no column numbers are
	 * supplied (i.e. no parameters) then no columns will be encoded.
	 * 
	 * @param columnsToEncode
	 *            the column numbers to encode
	 */
	public SelectiveCsvEncoder(final int... columnsToEncode) {
		if( columnsToEncode == null ) {
			throw new NullPointerException("columnsToEncode should not be null");
		}
		for( final Integer columnToEncode : columnsToEncode ) {
			columnNumbers.add(columnToEncode);
		}
	}
	
	/**
	 * Constructs a new <tt>SelectiveCsvEncoder</tt> that encodes columns if the element representing that column in the
	 * supplied array is true.
	 * 
	 * @param columnsToEncode
	 *            boolean array representing columns to encode (true indicates a column should be encoded).
	 */
	public SelectiveCsvEncoder(final boolean[] columnsToEncode) {
		if( columnsToEncode == null ) {
			throw new NullPointerException("columnsToEncode should not be null");
		}
		for( int i = 0; i < columnsToEncode.length; i++ ) {
			if( columnsToEncode[i] ) {
				columnNumbers.add(i + 1); // column numbers start at 1
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	public String encode(final String input, final CsvContext context, final CsvPreference preference) {
		return columnNumbers.contains(context.getColumnNumber()) ? super.encode(input, context, preference) : input;
		
	}
	
}
