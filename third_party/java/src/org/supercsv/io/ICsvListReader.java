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
 * Interface for readers that read into Lists.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public interface ICsvListReader extends ICsvReader {
	
	/**
	 * Reads a row of a CSV file and returns a List of Strings containing each column. If you are forced to use this
	 * method instead of {@link #read(CellProcessor...)} because your CSV file has a variable number of columns, then
	 * you can call the {@link #executeProcessors(CellProcessor...)} method after calling {@link #read()} to execute the
	 * cell processors manually (after determining the number of columns read in and which cell processors to use).
	 * 
	 * @return the List of columns, or null if EOF
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws SuperCsvException
	 *             if there was a general exception while reading/processing
	 * @since 1.0
	 */
	List<String> read() throws IOException;
	
	/**
	 * Reads a row of a CSV file and returns a List of Objects containing each column. The data can be further processed
	 * by cell processors (each element in the processors array corresponds with a CSV column). A <tt>null</tt> entry in
	 * the processors array indicates no further processing is required (the unprocessed String value will be added to
	 * the List). Prior to version 2.0.0 this method returned a List of Strings.
	 * 
	 * @param processors
	 *            an array of CellProcessors used to further process data before it is added to the List (each element
	 *            in the processors array corresponds with a CSV column - the number of processors should match the
	 *            number of columns). A <tt>null</tt> entry indicates no further processing is required (the unprocessed
	 *            String value will be added to the List).
	 * @return the List of columns, or null if EOF
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws NullPointerException
	 *             if processors is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if there was a general exception while reading/processing
	 * @since 1.0
	 */
	List<Object> read(CellProcessor... processors) throws IOException;
	
	/**
	 * Executes the supplied cell processors on the last row of CSV that was read. This should only be used when the
	 * number of CSV columns is unknown before the row is read, and you are forced to use {@link #read()} instead of
	 * {@link #read(CellProcessor...)}.
	 * 
	 * @param processors
	 *            an array of CellProcessors used to further process the last row of CSV data that was read (each
	 *            element in the processors array corresponds with a CSV column - the number of processors should match
	 *            the number of columns). A <tt>null</tt> entry indicates no further processing is required (the
	 *            unprocessed String value will be added to the List).
	 * @return the List of processed columns
	 * @throws NullPointerException
	 *             if processors is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if the wrong number of processors are supplied, or CellProcessor execution failed
	 * @since 2.1.0
	 */
	List<Object> executeProcessors(CellProcessor... processors);
}
