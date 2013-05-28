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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * The interface for CSV writers.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ICsvWriter extends Closeable, Flushable {
	
	/**
	 * Gets the current position in the file. The first line of the file is line number 1.
	 * 
	 * @since 1.0
	 */
	int getLineNumber();
	
	/**
	 * Gets the current row number (i.e. the number of CSV records - including the header - that have been written).
	 * This differs from the lineNumber, which is the number of real lines that have been written to the file. The first
	 * row is row 1 (which is typically the header row).
	 * 
	 * @since 2.0.0
	 */
	int getRowNumber();
	
	/**
	 * Writes a single-line comment to the CSV file (the comment must already include any special comment characters
	 * e.g. '#' at start). Please note that comments are not part of RFC4180, so this may make your CSV file less
	 * portable.
	 * 
	 * @param comment
	 *            the comment
	 * @throws NullPointerException
	 *             if comment is null
	 * @throws IOException
	 *             if an I/O error occurs
	 * @since 2.1.0
	 */
	void writeComment(String comment) throws IOException;
	
	/**
	 * Writes the header of the CSV file.
	 * 
	 * @param header
	 *            one or more header Strings
	 * @throws NullPointerException
	 *             if header is null
	 * @throws IOException
	 *             if an I/O error occurs
	 * @since 1.0
	 */
	void writeHeader(String... header) throws IOException;
	
}
