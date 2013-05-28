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
import java.util.Map;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.exception.SuperCsvException;

/**
 * The interface for writers that write from Maps.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ICsvMapWriter extends ICsvWriter {
	
	/**
	 * Writes the values of the Map as columns of a CSV file, using the supplied name mapping to map values to the
	 * appropriate columns. <tt>toString()</tt> will be called on each element prior to writing.
	 * 
	 * @param values
	 *            the Map containing the values to write
	 * @param nameMapping
	 *            an array of Strings linking the Map keys to their corresponding CSV columns (the array length should
	 *            match the number of columns). A <tt>null</tt> entry in the array indicates that the column should be
	 *            ignored (the column will be empty).
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws NullPointerException
	 *             if values or nameMapping are null
	 * @throws SuperCsvException
	 *             if there was a general exception while writing
	 * @since 1.0
	 */
	void write(Map<String, ?> values, String... nameMapping) throws IOException;
	
	/**
	 * Writes the values of the Map as columns of a CSV file, using the supplied name mapping to map values to the
	 * appropriate columns. <tt>toString()</tt> will be called on each element prior to writing.
	 * 
	 * @param values
	 *            the Map containing the values to write
	 * @param nameMapping
	 *            an array of Strings linking the Map keys to their corresponding CSV columns (the array length should
	 *            match the number of columns). A <tt>null</tt> entry in the array indicates that the column should be
	 *            ignored (the column will be empty).
	 * @param processors
	 *            an array of CellProcessors used to further process data before it is written (each element in the
	 *            processors array corresponds with a CSV column - the number of processors should match the number of
	 *            columns). A <tt>null</tt> entry indicates no further processing is required (the value returned by
	 *            toString() will be written as the column value).
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws NullPointerException
	 *             if values or nameMapping are null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if there was a general exception while writing
	 * @since 1.20
	 */
	void write(Map<String, ?> values, String[] nameMapping, CellProcessor[] processors) throws IOException;
	
}
