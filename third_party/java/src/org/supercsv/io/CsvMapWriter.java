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
import java.util.Map;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

/**
 * CsvMapWriter writes Maps of Objects to a CSV file.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvMapWriter extends AbstractCsvWriter implements ICsvMapWriter {
	
	// temporary storage of processed columns to be written
	private final List<Object> processedColumns = new ArrayList<Object>();
	
	/**
	 * Constructs a new <tt>CsvMapWriter</tt> with the supplied Writer and CSV preferences. Note that the
	 * <tt>writer</tt> will be wrapped in a <tt>BufferedWriter</tt> before accessed.
	 * 
	 * @param writer
	 *            the writer
	 * @param preference
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if writer or preference are null
	 * @since 1.0
	 */
	public CsvMapWriter(final Writer writer, final CsvPreference preference) {
		super(writer, preference);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final Map<String, ?> values, final String... nameMapping) throws IOException {
		super.incrementRowAndLineNo();
		super.writeRow(Util.filterMapToObjectArray(values, nameMapping));
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final Map<String, ?> values, final String[] nameMapping, final CellProcessor[] processors)
		throws IOException {
		
		super.incrementRowAndLineNo();
		
		// execute the processors for each column
		Util.executeCellProcessors(processedColumns, Util.filterMapToList(values, nameMapping), processors,
			getLineNumber(), getRowNumber());
		
		super.writeRow(processedColumns);
	}
}
