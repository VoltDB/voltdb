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

import java.util.HashMap;
import java.util.Set;

/**
 * A two-dimensional hashmap, is a HashMap that enables you to refer to values via two keys rather than one. The
 * underlying implementation is simply a HashMap containing HashMap, each of which maps to values.
 * 
 * @see java.util.HashMap
 * @author Kasper B. Graversen
 * @since 2.0.0 (migrated from Spiffy 0.5)
 */
public class TwoDHashMap<K1, K2, V> {
	
	private final HashMap<K1, HashMap<K2, V>> map;
	
	/**
	 * Constructs a new <tt>TwoDHashMap</tt>.
	 */
	public TwoDHashMap() {
		map = new HashMap<K1, HashMap<K2, V>>();
	}
	
	/**
	 * Constructs a new <tt>TwoDHashMap</tt> using the supplied map.
	 * 
	 * @param map
	 *            the map
	 * @throws NullPointerException
	 *             if map is null
	 */
	public TwoDHashMap(final HashMap<K1, HashMap<K2, V>> map) {
		if( map == null ) {
			throw new NullPointerException("map should not be null");
		}
		this.map = map;
	}
	
	/**
	 * Existence check of a value (or <tt>null</tt>) mapped to the keys.
	 * 
	 * @param firstKey
	 *            first key
	 * @param secondKey
	 *            second key
	 * @return true when an element (or <tt>null</tt>) has been stored with the keys
	 */
	public boolean containsKey(final K1 firstKey, final K2 secondKey) {
		// existence check on inner map
		final HashMap<K2, V> innerMap = map.get(firstKey);
		if( innerMap == null ) {
			return false;
		}
		return innerMap.containsKey(secondKey);
	}
	
	/**
	 * Fetch a value from the Hashmap .
	 * 
	 * @param firstKey
	 *            first key
	 * @param secondKey
	 *            second key
	 * @return the element or null.
	 */
	public V get(final K1 firstKey, final K2 secondKey) {
		// existence check on inner map
		final HashMap<K2, V> innerMap = map.get(firstKey);
		if( innerMap == null ) {
			return null;
		}
		return innerMap.get(secondKey);
	}
	
	/**
	 * Insert a value
	 * 
	 * @param firstKey
	 *            first key
	 * @param secondKey
	 *            second key
	 * @param value
	 *            the value to be inserted. <tt>null</tt> may be inserted as well.
	 * @return null or the value the insert is replacing.
	 */
	public Object set(final K1 firstKey, final K2 secondKey, final V value) {
		// existence check on inner map
		HashMap<K2, V> innerMap = map.get(firstKey);
		
		if( innerMap == null ) {
			// no inner map, create it
			innerMap = new HashMap<K2, V>();
			map.put(firstKey, innerMap);
		}
		
		return innerMap.put(secondKey, value);
	}
	
	/**
	 * Returns the number of key-value mappings in this map for the first key.
	 * 
	 * @return Returns the number of key-value mappings in this map for the first key.
	 */
	public int size() {
		return map.size();
	}
	
	/**
	 * Returns the number of key-value mappings in this map for the second key.
	 * 
	 * @return Returns the number of key-value mappings in this map for the second key.
	 */
	public int size(final K1 firstKey) {
		// existence check on inner map
		final HashMap<K2, V> innerMap = map.get(firstKey);
		if( innerMap == null ) {
			return 0;
		}
		return innerMap.size();
	}
	
	/**
	 * Returns a set of the keys of the outermost map.
	 */
	public Set<K1> keySet() {
		return map.keySet();
	}
	
}
