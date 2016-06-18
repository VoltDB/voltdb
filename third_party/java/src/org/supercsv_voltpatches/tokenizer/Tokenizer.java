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
package org.supercsv_voltpatches.tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.supercsv.comment.CommentMatcher;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.AbstractTokenizer;
import org.supercsv.prefs.CsvPreference;

/**
 * Reads the CSV file, line by line. If you want the line-reading functionality of this class, but want to define your
 * own implementation of {@link #readColumns(List)}, then consider writing your own Tokenizer by extending
 * AbstractTokenizer.
 *
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class Tokenizer extends AbstractTokenizer {

    private static final char NEWLINE = '\n';

    private static final char SPACE = ' ';

    private final StringBuilder currentColumn = new StringBuilder();

    /* the raw, untokenized CSV row (may span multiple lines) */
    private final StringBuilder currentRow = new StringBuilder();

    private final int quoteChar;

    private final int delimeterChar;

    private final boolean surroundingSpacesNeedQuotes;

    private final CommentMatcher commentMatcher;

    private final boolean strictQuotes;

    private final int escapeChar;

    private final long columnSizeLimit;

    private long skip;

    private boolean header;

    /**
     * Enumeration of tokenizer states. QUOTE_MODE is activated between quotes.
     */
    private enum TokenizerState {
	NORMAL, QUOTE_MODE;
    }

    /**
     * Constructs a new <tt>Tokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader
     *            the reader
     * @param preferences
     *            the CSV preferences
     * @throws NullPointerException
     *             if reader or preferences is null
     */
    public Tokenizer(final Reader reader, final CsvPreference preferences, boolean strictquotes, char escapechar,
            long columnsizelimit, long skipNum, boolean header) {
	super(reader, preferences);
	this.quoteChar = preferences.getQuoteChar();
	this.delimeterChar = preferences.getDelimiterChar();
	this.surroundingSpacesNeedQuotes = preferences.isSurroundingSpacesNeedQuotes();
	this.commentMatcher = preferences.getCommentMatcher();
	this.strictQuotes = strictquotes;
	this.escapeChar = escapechar;
	this.columnSizeLimit = columnsizelimit;
	this.skip = skipNum;
	this.header = header;
    }

    public Tokenizer(final Reader reader, final CsvPreference preferences, boolean strictquotes, char escapechar,
            long columnsizelimit, long skipNum) {
    this(reader, preferences, strictquotes, escapechar, columnsizelimit, skipNum, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean readColumns(final List<String> columns) throws IOException {

	if( columns == null ) {
	    throw new NullPointerException("columns should not be null");
	}

	// clear the reusable List and StringBuilders
	columns.clear();
	currentColumn.setLength(0);
	currentRow.setLength(0);

	// keep reading lines until data is found
	String line;

	do {
        line = readLine();
        if( line == null ) {
        return false; // EOF
        }
    }
    while( line.length() == 0 || line.trim().isEmpty() || (commentMatcher != null && commentMatcher.isComment(line))
            || (!header && skip >= getLineNumber()));

	if (header) {
	    header = false;
	    skip++;
	}

	// update the untokenized CSV row
	currentRow.append(line);

	// add a newline to determine end of line (making parsing easier)
	line += NEWLINE;

	// process each character in the line, catering for surrounding quotes (QUOTE_MODE)
	TokenizerState state = TokenizerState.NORMAL;
	int quoteScopeStartingLine = -1; // the line number where a potential multi-line cell starts
	int potentialSpaces = 0; // keep track of spaces (so leading/trailing space can be removed if required)
	int charIndex = 0;
	boolean espectQuote = this.strictQuotes;
	boolean isEscape = false;
	boolean sawNewLineInQuote = false;

	while( true ) {

	    final char c = line.charAt(charIndex);

	    if( TokenizerState.NORMAL.equals(state) ) {

		/*
		 * NORMAL mode (not within quotes).
		 */
		if( isEscape ) {
		    /*
		     * Just a normal character. Add any required spaces (but trim any leading spaces if surrounding
		     * spaces need quotes), add the character, then continue to next character.
		     */
		    isEscape = false;
		    if( this.strictQuotes && espectQuote ) {

			throw new SuperCsvException(
						    String.format(
								  "strictQuotes: quotes needed at line %d column %d. To proceed, " +
								  "either quote the column or remove --strictquotes",
								  getLineNumber(), columns.size() + 1));
		    }
		    if( !surroundingSpacesNeedQuotes || currentColumn.length() > 0 ) {
			appendSpaces(currentColumn, potentialSpaces);
		    }

		    potentialSpaces = 0;
		    currentColumn.append(c);
		}
		else if( c == escapeChar && !(line.charAt(charIndex+1) == 'N') ) {
		    isEscape = true;
		}

		else if( c == delimeterChar ) {
		    /*
		     * Delimiter. Save the column (trim trailing space if required) then continue to next character.
		     */
		    espectQuote = true;
		    if( !surroundingSpacesNeedQuotes ) {
			appendSpaces(currentColumn, potentialSpaces);
		    }
		    columns.add(currentColumn.length() > 0 ? currentColumn.toString() : null); // "" -> null
		    potentialSpaces = 0;
		    currentColumn.setLength(0);

		} else if( c == SPACE ) {
		    /*
		     * Space. Remember it, then continue to next character.
		     */
		    potentialSpaces++;

		} else if( c == NEWLINE ) {
		    /*
		     * Newline. Add any required spaces (if surrounding spaces don't need quotes) and return (we've read
		     * a line!).
		     */
		    if( !surroundingSpacesNeedQuotes ) {
			appendSpaces(currentColumn, potentialSpaces);
		    }
		    columns.add(currentColumn.length() > 0 ? currentColumn.toString() : null); // "" -> null
		    return true;

		} else if( c == quoteChar ) {
		    /*
		     * A single quote ("). Update to QUOTESCOPE (but don't save quote), then continue to next character.
		     */
		    espectQuote = false;
		    state = TokenizerState.QUOTE_MODE;
		    quoteScopeStartingLine = getLineNumber();

		    // cater for spaces before a quoted section (be lenient!)
		    if( !surroundingSpacesNeedQuotes || currentColumn.length() > 0 ) {
			appendSpaces(currentColumn, potentialSpaces);
		    }
		    potentialSpaces = 0;

		} else {
		    /*
		     * Just a normal character. Add any required spaces (but trim any leading spaces if surrounding
		     * spaces need quotes), add the character, then continue to next character.
		     */
		    if( this.strictQuotes && espectQuote ) {

			throw new SuperCsvException(
						    String.format(
								  "strictQuotes: quotes needed at line %d column %d. To proceed, " +
								  "either quote the column or remove --strictquotes",
								  getLineNumber(), columns.size() + 1));
		    }
		    if( !surroundingSpacesNeedQuotes || currentColumn.length() > 0 ) {
			appendSpaces(currentColumn, potentialSpaces);
		    }

		    potentialSpaces = 0;
		    currentColumn.append(c);
		}

	    } else {

		/*
		 * QUOTE_MODE (within quotes).
		 */
                if( sawNewLineInQuote ) {
                    if( currentColumn.length() > columnSizeLimit ) {
                        state = TokenizerState.NORMAL;
                        sawNewLineInQuote = false;
                        throw new SuperCsvException(
                                String
                                .format(
                                        "oversized column while reading quoted column %d beginning on line %d and ending on line %d. " +
                                        "See --columnsizelimit.",
                                        columns.size() + 1, quoteScopeStartingLine, getLineNumber()));
                    }
                }
		if( c == NEWLINE ) {

		    /*
		     * Newline. Doesn't count as newline while in QUOTESCOPE. Add the newline char, reset the charIndex
		     * (will update to 0 for next iteration), read in the next line, then then continue to next
		     * character. For a large file with an unterminated quoted section (no trailing quote), this could
		     * cause memory issues as it will keep reading lines looking for the trailing quote. Maybe there
		     * should be a configurable limit on max lines to read in quoted mode?
		     *
		     * Yes I'll set the limit to be 16*1024*1024B = 16MB by default
		     */
		    if( currentColumn.length() > columnSizeLimit ) {
		        state = TokenizerState.NORMAL;
		        sawNewLineInQuote = false;
		        throw new SuperCsvException(
		                String
		                .format(
		                        "oversized column while reading quoted column %d beginning on line %d and ending on line %d. " +
		                        "See --columnsizelimit.",
		                        columns.size() + 1, quoteScopeStartingLine, getLineNumber()));
		    }
		    sawNewLineInQuote = true;
		    currentColumn.append(NEWLINE);
		    currentRow.append(NEWLINE); // specific line terminator lost, \n will have to suffice

		    charIndex = -1;
		    line = readLine();
		    if( line == null ) {
			throw new SuperCsvException(
						    String
						    .format(
							    "unexpected end of file while reading quoted column %d beginning on line %d and ending on line %d",
							    columns.size() + 1, quoteScopeStartingLine, getLineNumber()));
		    }

		    currentRow.append(line); // update untokenized CSV row
		    line += NEWLINE; // add newline to simplify parsing
		} else if( c == quoteChar ) {

		    if( line.charAt(charIndex + 1) == quoteChar ) {
			/*
			 * An escaped quote (""). Add a single quote, then move the cursor so the next iteration of the
			 * loop will read the character following the escaped quote.
			 */
			currentColumn.append(c);
			charIndex++;

		    } else {
			/*
			 * A single quote ("). Update to NORMAL (but don't save quote), then continue to next character.
			 */
			state = TokenizerState.NORMAL;
			sawNewLineInQuote = false;
			quoteScopeStartingLine = -1; // reset ready for next multi-line cell

			// Check that we haven't gone over the column size limit
			if( currentColumn.length() > columnSizeLimit ) {
                state = TokenizerState.NORMAL;
                sawNewLineInQuote = false;
                throw new SuperCsvException(
                        String
                        .format(
                                "oversized column while reading quoted column %d beginning on line %d and ending on line %d. " +
                                "See --columnsizelimit.",
                                columns.size() + 1, quoteScopeStartingLine, getLineNumber()));
            }
		    }
		} else {
		    /*
		     * Just a normal character, delimiter (they don't count in QUOTESCOPE) or space. Add the character,
		     * then continue to next character.
		     */
		    currentColumn.append(c);
		}
	    }

	    charIndex++; // read next char of the line
	}
    }

    /**
     * Appends the required number of spaces to the StringBuilder.
     *
     * @param sb
     *            the StringBuilder
     * @param spaces
     *            the required number of spaces to append
     */
    private static void appendSpaces(final StringBuilder sb, final int spaces) {
	for( int i = 0; i < spaces; i++ ) {
	    sb.append(SPACE);
	}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUntokenizedRow() {
	return currentRow.toString();
    }
}
