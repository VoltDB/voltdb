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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvReflectionException;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.MethodCache;
import org.supercsv.util.Util;

/**
 * CsvBeanWriter writes a CSV file by mapping each field on the bean to a column in the CSV file (using the supplied
 * name mapping).
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvBeanWriter extends AbstractCsvWriter implements ICsvBeanWriter {
	
	// temporary storage of bean values
	private final List<Object> beanValues = new ArrayList<Object>();
	
	// temporary storage of processed columns to be written
	private final List<Object> processedColumns = new ArrayList<Object>();
	
	// cache of methods for mapping from fields to columns
	private final MethodCache cache = new MethodCache();
	
	/**
	 * Constructs a new <tt>CsvBeanWriter</tt> with the supplied Writer and CSV preferences. Note that the
	 * <tt>writer</tt> will be wrapped in a <tt>BufferedWriter</tt> before accessed.
	 * 
	 * @param writer
	 *            the writer
	 * @param preference
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if writer or preference are null
	 */
	public CsvBeanWriter(final Writer writer, final CsvPreference preference) {
		super(writer, preference);
	}
	
	/**
	 * Extracts the bean values, using the supplied name mapping array.
	 * 
	 * @param source
	 *            the bean
	 * @param nameMapping
	 *            the name mapping
	 * @throws NullPointerException
	 *             if source or nameMapping are null
	 * @throws SuperCsvReflectionException
	 *             if there was a reflection exception extracting the bean value
	 */
	private void extractBeanValues(final Object source, final String[] nameMapping) {
		
		if( source == null ) {
			throw new NullPointerException("the bean to write should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException(
				"the nameMapping array can't be null as it's used to map from fields to columns");
		}
		
		beanValues.clear();
		
		for( int i = 0; i < nameMapping.length; i++ ) {
			
			final String fieldName = nameMapping[i];
			
			if( fieldName == null ) {
				beanValues.add(null); // assume they always want a blank column
				
			} else {
				Method getMethod = cache.getGetMethod(source, fieldName);
				try {
					beanValues.add(getMethod.invoke(source));
				}
				catch(final Exception e) {
					throw new SuperCsvReflectionException(String.format("error extracting bean value for field %s",
						fieldName), e);
				}
			}
			
		}
		
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final Object source, final String... nameMapping) throws IOException {
		
		// update the current row/line numbers
		super.incrementRowAndLineNo();
		
		// extract the bean values
		extractBeanValues(source, nameMapping);
		
		// write the list
		super.writeRow(beanValues);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(final Object source, final String[] nameMapping, final CellProcessor[] processors)
		throws IOException {
		
		// update the current row/line numbers
		super.incrementRowAndLineNo();
		
		// extract the bean values
		extractBeanValues(source, nameMapping);
		
		// execute the processors for each column
		Util.executeCellProcessors(processedColumns, beanValues, processors, getLineNumber(), getRowNumber());
		
		// write the list
		super.writeRow(processedColumns);
	}
}
