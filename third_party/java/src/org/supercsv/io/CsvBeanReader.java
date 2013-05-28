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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.exception.SuperCsvReflectionException;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.BeanInterfaceProxy;
import org.supercsv.util.MethodCache;

/**
 * CsvBeanReader reads a CSV file by instantiating a bean for every row and mapping each column to a field on the bean
 * (using the supplied name mapping). The bean to populate can be either a class or interface. If a class is used, it
 * must be a valid Javabean, i.e. it must have a default no-argument constructor and getter/setter methods. An interface
 * may also be used if it defines getters/setters - a proxy object will be created that implements the interface.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvBeanReader extends AbstractCsvReader implements ICsvBeanReader {
	
	// temporary storage of processed columns to be mapped to the bean
	private final List<Object> processedColumns = new ArrayList<Object>();
	
	// cache of methods for mapping from columns to fields
	private final MethodCache cache = new MethodCache();
	
	/**
	 * Constructs a new <tt>CsvBeanReader</tt> with the supplied Reader and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public CsvBeanReader(final Reader reader, final CsvPreference preferences) {
		super(reader, preferences);
	}
	
	/**
	 * Constructs a new <tt>CsvBeanReader</tt> with the supplied (custom) Tokenizer and CSV preferences. The tokenizer
	 * should be set up with the Reader (CSV input) and CsvPreference beforehand.
	 * 
	 * @param tokenizer
	 *            the tokenizer
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if tokenizer or preferences are null
	 */
	public CsvBeanReader(final ITokenizer tokenizer, final CsvPreference preferences) {
		super(tokenizer, preferences);
	}
	
	/**
	 * Instantiates the bean (or creates a proxy if it's an interface).
	 * 
	 * @param clazz
	 *            the bean class to instantiate (a proxy will be created if an interface is supplied), using the default
	 *            (no argument) constructor
	 * @return the instantiated bean
	 * @throws SuperCsvReflectionException
	 *             if there was a reflection exception when instantiating the bean
	 */
	private static <T> T instantiateBean(final Class<T> clazz) {
		final T bean;
		if( clazz.isInterface() ) {
			bean = BeanInterfaceProxy.createProxy(clazz);
		} else {
			try {
				bean = clazz.newInstance();
			}
			catch(InstantiationException e) {
				throw new SuperCsvReflectionException(String.format(
					"error instantiating bean, check that %s has a default no-args constructor", clazz.getName()), e);
			}
			catch(IllegalAccessException e) {
				throw new SuperCsvReflectionException("error instantiating bean", e);
			}
		}
		
		return bean;
	}
	
	/**
	 * Invokes the setter on the bean with the supplied value.
	 * 
	 * @param bean
	 *            the bean
	 * @param setMethod
	 *            the setter method for the field
	 * @param fieldValue
	 *            the field value to set
	 * @throws SuperCsvException
	 *             if there was an exception invoking the setter
	 */
	private static void invokeSetter(final Object bean, final Method setMethod, final Object fieldValue) {
		try {
			setMethod.invoke(bean, fieldValue);
		}
		catch(final Exception e) {
			throw new SuperCsvReflectionException(String.format("error invoking method %s()", setMethod.getName()), e);
		}
	}
	
	/**
	 * Instantiates the bean (or creates a proxy if it's an interface), and maps the processed columns to the fields of
	 * the bean.
	 * 
	 * @param clazz
	 *            the bean class to instantiate (a proxy will be created if an interface is supplied), using the default
	 *            (no argument) constructor
	 * @param nameMapping
	 *            the name mappings
	 * @return the populated bean
	 * @throws SuperCsvReflectionException
	 *             if there was a reflection exception while populating the bean
	 */
	private <T> T populateBean(final Class<T> clazz, final String[] nameMapping) {
		
		// instantiate the bean or proxy
		final T resultBean = instantiateBean(clazz);
		
		// map each column to its associated field on the bean
		for( int i = 0; i < nameMapping.length; i++ ) {
			
			final Object fieldValue = processedColumns.get(i);
			
			// don't call a set-method in the bean if there is no name mapping for the column or no result to store
			if( nameMapping[i] == null || fieldValue == null ) {
				continue;
			}
			
			// invoke the setter on the bean
			Method setMethod = cache.getSetMethod(resultBean, nameMapping[i], fieldValue.getClass());
			invokeSetter(resultBean, setMethod, fieldValue);
			
		}
		
		return resultBean;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public <T> T read(final Class<T> clazz, final String... nameMapping) throws IOException {
		
		if( clazz == null ) {
			throw new NullPointerException("clazz should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		}
		
		if( readRow() ) {
			if( nameMapping.length != length() ) {
				throw new IllegalArgumentException(String.format("the nameMapping array and the number of columns read "
					+ "should be the same size (nameMapping length = %d, columns = %d)", nameMapping.length, length()));
			}
			processedColumns.clear();
			processedColumns.addAll(getColumns());
			return populateBean(clazz, nameMapping);
		}
		
		return null; // EOF
	}
	
	/**
	 * {@inheritDoc}
	 */
	public <T> T read(final Class<T> clazz, final String[] nameMapping, final CellProcessor... processors)
		throws IOException {
		
		if( clazz == null ) {
			throw new NullPointerException("clazz should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		} else if( processors == null ) {
			throw new NullPointerException("processors should not be null");
		}
		
		if( readRow() ) {
			// execute the processors then populate the bean
			executeProcessors(processedColumns, processors);
			return populateBean(clazz, nameMapping);
		}
		
		return null; // EOF
	}
	
}
