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
import java.io.IOException;
import java.util.List;

import org.supercsv.exception.SuperCsvException;

/**
 * The interface for tokenizers, which are responsible for reading the CSV file, line by line.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ITokenizer extends Closeable {
	
	/**
	 * Gets the line number currently being tokenized (the first line is line 1). This number increments at every line
	 * terminator as the data is read, i.e. it will be
	 * <ul>
	 * <li>0, if {@link #readColumns(List)} hasn't been called yet</li>
	 * <li>1, when the first line is being read/tokenized</li>
	 * <li>2, when the second line is being read/tokenized</li>
	 * </ul>
	 * 
	 * @return the line number currently being tokenized
	 */
	int getLineNumber();
	
	/**
	 * Returns the raw (untokenized) CSV row that was just read (which can potentially span multiple lines in the file).
	 * 
	 * @return the raw (untokenized) CSV row that was just read
	 * @since 2.0.0
	 */
	String getUntokenizedRow();
	
	/**
	 * Reads a CSV row into the supplied List of columns (which can potentially span multiple lines in the file). The
	 * columns list is cleared as the first operation in the method. Any empty columns ("") will be added to the list as
	 * <tt>null</tt>.
	 * 
	 * @param columns
	 *            the List of columns to read into
	 * @return true if something was read, or false if EOF
	 * @throws IOException
	 *             when an IOException occurs
	 * @throws NullPointerException
	 *             if columns is null
	 * @throws SuperCsvException
	 *             on errors in parsing the input
	 * @since 2.0.0 (was previously called readStringList)
	 */
	boolean readColumns(List<String> columns) throws IOException;
}
