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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * CommentMatcher that matches lines that match a specified regular expression.
 */
public class CommentMatches implements CommentMatcher {
	
	private final Pattern pattern;
	
	/**
	 * Constructs a new <tt>CommentMatches</tt> comment matcher. Ensure that the regex is efficient (ideally matching start/end
	 * characters) as a complex regex can significantly slow down reading.
	 * 
	 * @param regex
	 *            the regular expression a line must match to be a comment
	 * @throws NullPointerException
	 *             if regex is null
	 * @throws IllegalArgumentException
	 *             if regex is empty
	 * @throws PatternSyntaxException
	 *             if the regex is invalid
	 */
	public CommentMatches(final String regex) {
		if( regex == null ) {
			throw new NullPointerException("regex should not be null");
		} else if( regex.length() == 0 ) {
			throw new IllegalArgumentException("regex should not be empty");
		}
		this.pattern = Pattern.compile(regex);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean isComment(String line) {
		return pattern.matcher(line).matches();
	}
	
}
