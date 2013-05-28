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
package org.supercsv.cellprocessor;

import org.supercsv.cellprocessor.ift.CellProcessor;

/**
 * This processor is used to indicate that a cell is optional, and will avoid executing further processors if it
 * encounters <tt>null</tt>. It is a simple customization of <tt>ConvertNullTo</tt>.
 * <p>
 * Prior to version 2.0.0, this processor returned <tt>null</tt> for empty String (""), but was updated because
 * Tokenizer now reads empty columns as <tt>null</tt>. It also means that Optional can now be used when writing as well
 * (instead of using {@code ConvertNullTo("")}).
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class Optional extends ConvertNullTo {
	
	/**
	 * Constructs a new <tt>Optional</tt> processor, which when encountering <tt>null</tt> will return <tt>null</tt>,
	 * for all other values it will return the value unchanged.
	 */
	public Optional() {
		super(null);
	}
	
	/**
	 * Constructs a new <tt>Optional</tt> processor, which when encountering <tt>null</tt> will return <tt>null</tt> ,
	 * for all other values it will call the next processor in the chain.
	 * 
	 * @throws NullPointerException
	 *             if next is null
	 */
	public Optional(final CellProcessor next) {
		super(null, next);
	}
	
}
