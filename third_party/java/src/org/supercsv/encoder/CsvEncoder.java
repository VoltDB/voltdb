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
package org.supercsv.encoder;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * Defines the interface for all CSV encoders.
 * 
 * @author James Bassett
 * @since 2.1.0
 */
public interface CsvEncoder {
	
	/**
	 * Encodes a String to be written to a CSV file. The encoder should honour all CSV preferences including updating
	 * the current lineNumber (in the CSV context - it will be updated in the calling CsvWriter after encoding has
	 * completed) as line terminators are encountered in the String to be escaped (converting all 3 variations of line
	 * terminators to the end of line symbols specified in the preferences). The CsvContext can also be used to encode
	 * based on the current context (e.g. you may want to always put quotes around a particular column).
	 * 
	 * @param input
	 *            the String to be encoded
	 * @param context
	 *            the context
	 * @param preference
	 *            the CSV preferences
	 * @return the encoded CSV
	 */
	String encode(String input, CsvContext context, CsvPreference preference);
	
}
