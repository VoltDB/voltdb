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
package org.supercsv.comment;

/**
 * CommentMatcher that matches lines that begin with a specified String.
 */
public class CommentStartsWith implements CommentMatcher {
	
	private final String value;
	
	/**
	 * Constructs a new <tt>CommentStartsWith</tt> comment matcher.
	 * 
	 * @param value
	 *            the String a line must start with to be a comment
	 * @throws NullPointerException
	 *             if value is null
	 * @throws IllegalArgumentException
	 *             if value is empty
	 */
	public CommentStartsWith(final String value) {
		if( value == null ) {
			throw new NullPointerException("value should not be null");
		} else if( value.length() == 0 ) {
			throw new IllegalArgumentException("value should not be empty");
		}
		this.value = value;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean isComment(String line) {
		return line.startsWith(value);
	}
	
}
