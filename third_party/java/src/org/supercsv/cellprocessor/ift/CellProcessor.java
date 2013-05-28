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
package org.supercsv.cellprocessor.ift;

import org.supercsv.util.CsvContext;

/**
 * Defines the interface of all <tt>CellProcessor</tt>s.
 */
public interface CellProcessor {
	
	/**
	 * This method is invoked by the framework when the processor needs to process data or check constraints.
	 * 
	 * @since 1.0
	 */
	Object execute(final Object value, final CsvContext context);
}
