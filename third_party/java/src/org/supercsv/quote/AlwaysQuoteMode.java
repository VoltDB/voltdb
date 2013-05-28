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
package org.supercsv.quote;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * When using AlwaysQuoteMode surrounding quotes are always applied.
 * 
 * @author James Bassett
 * @since 2.1.0
 */
public class AlwaysQuoteMode implements QuoteMode {
	
	/**
	 * Constructs a new <tt>AlwaysQuoteMode</tt>.
	 */
	public AlwaysQuoteMode() {
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean quotesRequired(final String csvColumn, final CsvContext context, final CsvPreference preference) {
		return true;
	}
	
}
