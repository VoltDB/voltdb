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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.supercsv.encoder.CsvEncoder;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;
import org.supercsv.util.Util;

/**
 * Defines the standard behaviour of a CSV writer.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public abstract class AbstractCsvWriter implements ICsvWriter {
	
	private final BufferedWriter writer;
	
	private final CsvPreference preference;
	
	private final CsvEncoder encoder;
	
	// the line number being written / just written
	private int lineNumber = 0;
	
	// the row being written / just written
	private int rowNumber = 0;
	
	// the column being written / just written
	private int columnNumber = 0;
	
	/**
	 * Constructs a new <tt>AbstractCsvWriter</tt> with the supplied writer and preferences.
	 * 
	 * @param writer
	 *            the stream to write to
	 * @param preference
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if writer or preference are null
	 */
	public AbstractCsvWriter(final Writer writer, final CsvPreference preference) {
		if( writer == null ) {
			throw new NullPointerException("writer should not be null");
		} else if( preference == null ) {
			throw new NullPointerException("preference should not be null");
		}
		
		this.writer = new BufferedWriter(writer);
		this.preference = preference;
		this.encoder = preference.getEncoder();
	}
	
	/**
	 * Closes the underlying writer, flushing it first.
	 */
	public void close() throws IOException {
		writer.close();
	}
	
	/**
	 * Flushes the underlying writer.
	 */
	public void flush() throws IOException {
		writer.flush();
	}
	
	/**
	 * Make a string ready for writing by escaping various characters as specified by the CSV format. This method also
	 * updates the current lineNumber as line terminators are encountered in the String to be escaped (converting all 3
	 * variations of line terminators to the end of line symbols specified in the preferences).
	 * 
	 * @param csvElement
	 *            an element of a CSV file
	 * @return an escaped version of the element ready for writing
	 * @deprecated this method will be removed in the next major release, in favour of supplying your own CsvEncoder via
	 *             the CsvPreference object. If you have overridden this method, it will not delegate to the CsvEncoder
	 *             at all.
	 */
	protected String escapeString(final String csvElement) {
		// TODO move this code to writeRow() when this method is removed
		final CsvContext context = new CsvContext(lineNumber, rowNumber, columnNumber);
		final String escapedCsv = encoder.encode(csvElement, context, preference);
		lineNumber = context.getLineNumber(); // line number can increment when encoding multi-line columns
		return escapedCsv;
	}
	
	/**
	 * In order to maintain the current row and line numbers, this method <strong>must</strong> be called at the very
	 * beginning of every write method implemented in concrete CSV writers. This will allow the correct row/line numbers
	 * to be used in any exceptions thrown before writing occurs (e.g. during CellProcessor execution), and means that
	 * {@link #getLineNumber()} and {@link #getRowNumber()} can be called after writing to return the line/row just
	 * written.
	 */
	protected void incrementRowAndLineNo() {
		lineNumber++;
		rowNumber++;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public int getLineNumber() {
		return lineNumber;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public int getRowNumber() {
		return rowNumber;
	}
	
	/**
	 * Writes a List of columns as a line to the CsvWriter.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.size == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 */
	protected void writeRow(final List<?> columns) throws IOException {
		writeRow(Util.objectListToStringArray(columns));
	}
	
	/**
	 * Writes one or more Object columns as a line to the CsvWriter.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.length == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 */
	protected void writeRow(final Object... columns) throws IOException {
		writeRow(Util.objectArrayToStringArray(columns));
	}
	
	/**
	 * Writes one or more String columns as a line to the CsvWriter.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.length == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 */
	protected void writeRow(final String... columns) throws IOException {
		
		if( columns == null ) {
			throw new NullPointerException(String.format("columns to write should not be null on line %d", lineNumber));
		} else if( columns.length == 0 ) {
			throw new IllegalArgumentException(String.format("columns to write should not be empty on line %d",
				lineNumber));
		}
		
		for( int i = 0; i < columns.length; i++ ) {
			
			columnNumber = i + 1; // column no used by CsvEncoder
			
			if( i > 0 ) {
				writer.write(preference.getDelimiterChar()); // delimiter
			}
			
			final String csvElement = columns[i];
			if( csvElement != null ) {
				writer.write(escapeString(csvElement)); // escaped column (a null column implies "")
			}
			
		}
		
		writer.write(preference.getEndOfLineSymbols()); // EOL
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void writeComment(final String comment) throws IOException {
		
		lineNumber++; // we're not catering for embedded newlines (must be a single-line comment)
		
		if( comment == null ) {
			throw new NullPointerException(String.format("comment to write should not be null on line %d", lineNumber));
		}
		
		writer.write(comment);
		writer.write(preference.getEndOfLineSymbols());
		
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void writeHeader(final String... header) throws IOException {
		
		// update the current row/line numbers
		incrementRowAndLineNo();
		
		writeRow(header);
	}
	
}
