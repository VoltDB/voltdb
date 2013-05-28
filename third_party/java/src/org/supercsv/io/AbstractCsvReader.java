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
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

/**
 * Defines the standard behaviour of a CSV reader.
 *
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public abstract class AbstractCsvReader implements ICsvReader {

	private final ITokenizer tokenizer;

	private final CsvPreference preferences;

	// the current tokenized columns
	private final List<String> columns = new ArrayList<String>();

	// the number of CSV records read
	private int rowNumber = 0;

	private boolean EOF = false;

	/**
	 * Constructs a new <tt>AbstractCsvReader</tt>, using the default {@link Tokenizer}.
	 *
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public AbstractCsvReader(final Reader reader, final CsvPreference preferences) {
		if( reader == null ) {
			throw new NullPointerException("reader should not be null");
		} else if( preferences == null ) {
			throw new NullPointerException("preferences should not be null");
		}

		this.preferences = preferences;
		this.tokenizer = new Tokenizer(reader, preferences);
	}

	/**
	 * Constructs a new <tt>AbstractCsvReader</tt>, using a custom {@link Tokenizer} (which should have already been set
	 * up with the Reader, CsvPreference, and CsvContext). This constructor should only be used if the default Tokenizer
	 * doesn't provide the required functionality.
	 *
	 * @param tokenizer
	 *            the tokenizer
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if tokenizer or preferences are null
	 */
	public AbstractCsvReader(final ITokenizer tokenizer, final CsvPreference preferences) {
		if( tokenizer == null ) {
			throw new NullPointerException("tokenizer should not be null");
		} else if( preferences == null ) {
			throw new NullPointerException("preferences should not be null");
		}

		this.preferences = preferences;
		this.tokenizer = tokenizer;
	}

	/**
	 * Closes the Tokenizer and its associated Reader.
	 */
	@Override
	public void close() throws IOException {
		tokenizer.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String get(final int n) {
		return columns.get(n - 1); // column numbers start at 1
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getHeader(final boolean firstLineCheck) throws IOException {

		if( firstLineCheck && tokenizer.getLineNumber() != 0 ) {
			throw new SuperCsvException(String.format(
				"CSV header must be fetched as the first read operation, but %d lines have already been read",
				tokenizer.getLineNumber()));
		}

		if( readRow() ) {
			return columns.toArray(new String[columns.size()]);
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getLineNumber() {
		return tokenizer.getLineNumber();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUntokenizedRow() {
		return tokenizer.getUntokenizedRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRowNumber() {
		return rowNumber;
	}

	@Override
	public int incrementRowNumber() {
		return ++rowNumber;
	}

	@Override
	public boolean isEOF() {
		return EOF;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int length() {
		return columns.size();
	}

	/**
	 * Gets the tokenized columns.
	 *
	 * @return the tokenized columns
	 */
	protected List<String> getColumns() {
		return columns;
	}

	/**
	 * Gets the preferences.
	 *
	 * @return the preferences
	 */
	protected CsvPreference getPreferences() {
		return preferences;
	}

	/**
	 * Calls the tokenizer to read a CSV row. The columns can then be retrieved using {@link #getColumns()}.
	 *
	 * @return true if something was read, and false if EOF
	 * @throws IOException
	 *             when an IOException occurs
	 * @throws SuperCsvException
	 *             on errors in parsing the input
	 */
	protected boolean readRow() throws IOException {
		if( tokenizer.readColumns(columns) ) {
			rowNumber++;
			return true;
		}
		EOF = true;
		return false;
	}

	/**
	 * Executes the supplied cell processors on the last row of CSV that was read and populates the supplied List of
	 * processed columns.
	 *
	 * @param processedColumns
	 *            the List to populate with processed columns
	 * @param processors
	 *            the cell processors
	 * @return the updated List
	 * @throws NullPointerException
	 *             if processedColumns or processors is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if the wrong number of processors are supplied, or CellProcessor execution failed
	 */
	protected List<Object> executeProcessors(final List<Object> processedColumns, final CellProcessor[] processors) {
		Util.executeCellProcessors(processedColumns, getColumns(), processors, getLineNumber(), getRowNumber());
		return processedColumns;
	}

}
