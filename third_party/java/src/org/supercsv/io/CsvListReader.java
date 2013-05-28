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
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;

/**
 * CsvListReader is a simple reader that reads a row from a CSV file into a <tt>List</tt> of Strings.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvListReader extends AbstractCsvReader implements ICsvListReader {
	
	/**
	 * Constructs a new <tt>CsvListReader</tt> with the supplied Reader and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public CsvListReader(final Reader reader, final CsvPreference preferences) {
		super(reader, preferences);
	}
	
	/**
	 * Constructs a new <tt>CsvListReader</tt> with the supplied (custom) Tokenizer and CSV preferences. The tokenizer
	 * should be set up with the Reader (CSV input) and CsvPreference beforehand.
	 * 
	 * @param tokenizer
	 *            the tokenizer
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if tokenizer or preferences are null
	 */
	public CsvListReader(final ITokenizer tokenizer, final CsvPreference preferences) {
		super(tokenizer, preferences);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public List<String> read() throws IOException {
		
		if( readRow() ) {
			return new ArrayList<String>(getColumns());
		}
		
		return null; // EOF
	}
	
	/**
	 * {@inheritDoc}
	 */
	public List<Object> read(final CellProcessor... processors) throws IOException {
		
		if( processors == null ) {
			throw new NullPointerException("processors should not be null");
		}
		
		if( readRow() ) {
			return executeProcessors(processors);
		}
		
		return null; // EOF
	}
	
	/**
	 * {@inheritDoc}
	 */
	public List<Object> executeProcessors(final CellProcessor... processors) {
		return super.executeProcessors(new ArrayList<Object>(getColumns().size()), processors);
	}
}
