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
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

/**
 * CsvListWriter is a simple writer capable of writing arrays and Lists to a CSV file.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvListWriter extends AbstractCsvWriter implements ICsvListWriter {
	
	// temporary storage of processed columns to be written
	private final List<Object> processedColumns = new ArrayList<Object>();
	
	/**
	 * Constructs a new <tt>CsvListWriter</tt> with the supplied Writer and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param writer
	 *            the writer
	 * @param preference
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if writer or preference are null
	 */
	public CsvListWriter(final Writer writer, final CsvPreference preference) {
		super(writer, preference);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final List<?> columns, final CellProcessor[] processors) throws IOException {
		
		super.incrementRowAndLineNo();
		
		// execute the processors for each column
		Util.executeCellProcessors(processedColumns, columns, processors, getLineNumber(), getRowNumber());
		
		super.writeRow(processedColumns);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(List<?> columns) throws IOException {
		super.incrementRowAndLineNo();
		super.writeRow(columns);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final Object... columns) throws IOException {
		super.incrementRowAndLineNo();
		super.writeRow(columns);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final String... columns) throws IOException {
		super.incrementRowAndLineNo();
		super.writeRow(columns);
	}
}
