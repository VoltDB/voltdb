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
package org.supercsv.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvConstraintViolationException;
import org.supercsv.exception.SuperCsvException;

/**
 * Useful utility methods.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public final class Util {
	
	// no instantiation
	private Util() {
	}
	
	/**
	 * Processes each element in the source List (using the corresponding processor chain in the processors array) and
	 * adds it to the destination List. A <tt>null</tt> CellProcessor in the array indicates that no processing is
	 * required and the element should be added as-is.
	 * 
	 * @param destination
	 *            the List to add the processed elements to (which is cleared before it's populated)
	 * @param source
	 *            the List of source elements to be processed
	 * @param processors
	 *            the array of CellProcessors used to process each element. The number of elements in this array must
	 *            match the size of the source List. A <tt>null</tt> CellProcessor in this array indicates that no
	 *            processing is required and the element should be added as-is.
	 * @param lineNo
	 *            the current line number
	 * @param rowNo
	 *            the current row number
	 * @throws NullPointerException
	 *             if destination, source or processors is null
	 * @throws SuperCsvConstraintViolationException
	 *             if a CellProcessor constraint failed
	 * @throws SuperCsvException
	 *             if source.size() != processors.length, or CellProcessor execution failed
	 */
	public static void executeCellProcessors(final List<Object> destination, final List<?> source,
		final CellProcessor[] processors, final int lineNo, final int rowNo) {
		
		if( destination == null ) {
			throw new NullPointerException("destination should not be null");
		} else if( source == null ) {
			throw new NullPointerException("source should not be null");
		} else if( processors == null ) {
			throw new NullPointerException("processors should not be null");
		}
		
		// the context used when cell processors report exceptions
		final CsvContext context = new CsvContext(lineNo, rowNo, 1);
		context.setRowSource(new ArrayList<Object>(source));
		
		if( source.size() != processors.length ) {
			throw new SuperCsvException(String.format(
				"The number of columns to be processed (%d) must match the number of CellProcessors (%d): check that the number"
					+ " of CellProcessors you have defined matches the expected number of columns being read/written",
				source.size(), processors.length), context);
		}
		
		destination.clear();
		
		for( int i = 0; i < source.size(); i++ ) {
			
			context.setColumnNumber(i + 1); // update context (columns start at 1)
			
			if( processors[i] == null ) {
				destination.add(source.get(i)); // no processing required
			} else {
				destination.add(processors[i].execute(source.get(i), context)); // execute the processor chain
			}
		}
	}
	
	/**
	 * Converts a List to a Map using the elements of the nameMapping array as the keys of the Map.
	 * 
	 * @param destinationMap
	 *            the destination Map (which is cleared before it's populated)
	 * @param nameMapping
	 *            the keys of the Map (corresponding with the elements in the sourceList). Cannot contain duplicates.
	 * @param sourceList
	 *            the List to convert
	 * @throws NullPointerException
	 *             if destinationMap, nameMapping or sourceList are null
	 * @throws SuperCsvException
	 *             if nameMapping and sourceList are not the same size
	 */
	public static <T> void filterListToMap(final Map<String, T> destinationMap, final String[] nameMapping,
		final List<? extends T> sourceList) {
		if( destinationMap == null ) {
			throw new NullPointerException("destinationMap should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		} else if( sourceList == null ) {
			throw new NullPointerException("sourceList should not be null");
		} else if( nameMapping.length != sourceList.size() ) {
			throw new SuperCsvException(
				String
					.format(
						"the nameMapping array and the sourceList should be the same size (nameMapping length = %d, sourceList size = %d)",
						nameMapping.length, sourceList.size()));
		}
		
		destinationMap.clear();
		
		for( int i = 0; i < nameMapping.length; i++ ) {
			final String key = nameMapping[i];
			
			if( key == null ) {
				continue; // null's in the name mapping means skip column
			}
			
			// no duplicates allowed
			if( destinationMap.containsKey(key) ) {
				throw new SuperCsvException(String.format("duplicate nameMapping '%s' at index %d", key, i));
			}
			
			destinationMap.put(key, sourceList.get(i));
		}
	}
	
	/**
	 * Returns a List of all of the values in the Map whose key matches an entry in the nameMapping array.
	 * 
	 * @param map
	 *            the map
	 * @param nameMapping
	 *            the keys of the Map values to add to the List
	 * @return a List of all of the values in the Map whose key matches an entry in the nameMapping array
	 * @throws NullPointerException
	 *             if map or nameMapping is null
	 */
	public static List<Object> filterMapToList(final Map<String, ?> map, final String[] nameMapping) {
		if( map == null ) {
			throw new NullPointerException("map should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		}
		
		final List<Object> result = new ArrayList<Object>(nameMapping.length);
		for( final String key : nameMapping ) {
			result.add(map.get(key));
		}
		return result;
	}
	
	/**
	 * Converts a Map to an array of objects, adding only those entries whose key is in the nameMapping array.
	 * 
	 * @param values
	 *            the Map of values to convert
	 * @param nameMapping
	 *            the keys to extract from the Map (elements in the target array will be added in this order)
	 * @return the array of Objects
	 * @throws NullPointerException
	 *             if values or nameMapping is null
	 */
	public static Object[] filterMapToObjectArray(final Map<String, ?> values, final String[] nameMapping) {
		
		if( values == null ) {
			throw new NullPointerException("values should not be null");
		} else if( nameMapping == null ) {
			throw new NullPointerException("nameMapping should not be null");
		}
		
		final Object[] targetArray = new Object[nameMapping.length];
		int i = 0;
		for( final String name : nameMapping ) {
			targetArray[i++] = values.get(name);
		}
		return targetArray;
	}
	
	/**
	 * Converts an Object array to a String array (null-safe), by calling toString() on each element.
	 * 
	 * @param objectArray
	 *            the Object array
	 * @return the String array, or null if objectArray is null
	 */
	public static String[] objectArrayToStringArray(final Object[] objectArray) {
		if( objectArray == null ) {
			return null;
		}
		
		final String[] stringArray = new String[objectArray.length];
		for( int i = 0; i < objectArray.length; i++ ) {
			stringArray[i] = objectArray[i] != null ? objectArray[i].toString() : null;
		}
		
		return stringArray;
	}
	
	/**
	 * Converts an List<Object) to a String array (null-safe), by calling toString() on each element.
	 * 
	 * @param objectList
	 *            the List
	 * @return the String array, or null if objectList is null
	 */
	public static String[] objectListToStringArray(final List<?> objectList) {
		if( objectList == null ) {
			return null;
		}
		
		final String[] stringArray = new String[objectList.size()];
		for( int i = 0; i < objectList.size(); i++ ) {
			stringArray[i] = objectList.get(i) != null ? objectList.get(i).toString() : null;
		}
		
		return stringArray;
	}
	
}
