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
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.exception.SuperCsvException;

/**
 * Interface for writers that write to a List.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ICsvListWriter extends ICsvWriter {
	
	/**
	 * Writes a List of Objects as columns of a CSV file. <tt>toString()</tt> will be called on each element prior to
	 * writing.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.size == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 * @throws SuperCsvException
	 *             if there was a general exception while writing
	 * @since 1.0
	 */
	void write(List<?> columns) throws IOException;
	
	/**
	 * Writes a List of Objects as columns of a CSV file, performing any necessary processing beforehand.
	 * <tt>toString()</tt> will be called on each (processed) element prior to writing.
	 * 
	 * @param columns
	 *            the columns to write
	 * @param processors
	 *            an array of CellProcessors used to further process data before it is written (each element in the
	 *            processors array corresponds with a CSV column - the number of processors should match the number of
	 *            columns). A <tt>null</tt> entry indicates no further processing is required (the value returned by
	 *            toString() will be written as the column value).
	 * @throws IllegalArgumentException
	 *             if columns.size == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns or processors is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if there was a general exception while writing/processing
	 * @since 1.0
	 */
	void write(List<?> columns, CellProcessor[] processors) throws IOException;
	
	/**
	 * Writes a array of Objects as columns of a CSV file. <tt>toString()</tt> will be called on each element prior to
	 * writing.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.length == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 * @throws SuperCsvException
	 *             if there was a general exception while writing
	 * @since 1.0
	 */
	void write(Object... columns) throws IOException;
	
	/**
	 * Writes an array of strings as columns of a CSV file.
	 * 
	 * @param columns
	 *            the columns to write
	 * @throws IllegalArgumentException
	 *             if columns.length == 0
	 * @throws IOException
	 *             If an I/O error occurs
	 * @throws NullPointerException
	 *             if columns is null
	 * @throws SuperCsvException
	 *             if there was a general exception while writing
	 * @since 1.0
	 */
	void write(String... columns) throws IOException;
	
}
