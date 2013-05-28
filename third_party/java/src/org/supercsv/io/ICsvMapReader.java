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
 * The interface for MapReaders, which read each CSV row into a Map.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ICsvMapReader extends ICsvReader {
	
	/**
	 * Reads a row of a CSV file into a Map, using the supplied name mapping to map column values to the appropriate map
	 * entries.
	 * 
	 * @param nameMapping
	 *            an array of Strings linking the CSV columns to their corresponding entry in the Map (the array length
	 *            should match the number of columns). A <tt>null</tt> entry in the array indicates that the column
	 *            should be ignored (e.g. not added to the Map).
	 * @return a Map of column names to column values (Strings, as no processing is performed), or null if EOF
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws NullPointerException
	 *             if nameMapping is null
	 * @throws SuperCsvException
	 *             if there was a general exception while reading/processing
	 * @since 1.0
	 */
	Map<String, String> read(String... nameMapping) throws IOException;
	
	/**
	 * Reads a row of a CSV file into a Map, using the supplied name mapping to map column values to the appropriate map
	 * entries, and the supplied processors to process the values before adding them to the Map.
	 * 
	 * @param nameMapping
	 *            an array of Strings linking the CSV columns to their corresponding entry in the Map (the array length
	 *            should match the number of columns). A <tt>null</tt> entry in the array indicates that the column
	 *            should be ignored (e.g. not added to the Map).
	 * @param processors
	 *            an array of CellProcessors used to further process data before it is added to the Map (each element in
	 *            the processors array corresponds with a CSV column - the number of processors should match the number
	 *            of columns). A <tt>null</tt> entry indicates no further processing is required (the unprocessed String
	 *            value will added to the Map).
	 * @return a Map of column names to column values, or null if EOF
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws NullPointerException
	 *             if nameMapping or processors are null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if there was a general exception while reading/processing
	 * @since 1.0
	 */
	Map<String, Object> read(String[] nameMapping, CellProcessor[] processors) throws IOException;
	
}
