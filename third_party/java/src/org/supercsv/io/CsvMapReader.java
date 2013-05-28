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
package org.supercsv.io;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

/**
 * CsvMapReader reads each CSV row into a Map with the column name as the map key, and the column value as the map
 * value.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvMapReader extends AbstractCsvReader implements ICsvMapReader {
	
	/**
	 * Constructs a new <tt>CsvMapReader</tt> with the supplied Reader and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public CsvMapReader(final Reader reader, final CsvPreference preferences) {
		super(reader, preferences);
	}
	
	/**
	 * Constructs a new <tt>CsvMapReader</tt> with the supplied (custom) Tokenizer and CSV preferences. The tokenizer
	 * should be set up with the Reader (CSV input) and CsvPreference beforehand.
	 * 
	 * @param tokenizer
	 *            the tokenizer
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if tokenizer or preferences are null
	 */
	public CsvMapReader(final ITokenizer tokenizer, final CsvPreference preferences) {
		super(tokenizer, preferences);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Map<String, String> read(final String... nameMapping) throws IOException {
		
		if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		}
		
		if( readRow() ) {
			final Map<String, String> destination = new HashMap<String, String>();
			Util.filterListToMap(destination, nameMapping, getColumns());
			return destination;
		}
		
		return null; // EOF
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Map<String, Object> read(final String[] nameMapping, final CellProcessor[] processors) throws IOException {
		
		if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		} else if( processors == null ) {
			throw new NullPointerException("processors should not be null");
		}
		
		if( readRow() ) {
			// process the columns
			final List<Object> processedColumns = executeProcessors(new ArrayList<Object>(getColumns().size()),
				processors);
			
			// convert the List to a Map
			final Map<String, Object> destination = new HashMap<String, Object>(processedColumns.size());
			Util.filterListToMap((Map<String, Object>) destination, nameMapping, (List<Object>) processedColumns);
			return destination;
		}
		
		return null; // EOF
	}
}
