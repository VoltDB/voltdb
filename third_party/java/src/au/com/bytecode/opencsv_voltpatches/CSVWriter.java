package au.com.bytecode.opencsv_voltpatches;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 * @author Glen Smith
 *
 */
public class CSVWriter implements Closeable {

    public static final int INITIAL_STRING_SIZE = 128;

    private Writer rawWriter;

    private PrintWriter pw;

    private final char separator;

    private final char quotechar;

    private final char escapechar;

    private final String lineEnd;

    private final boolean quoteAll;

    private final String nullString;

    private char[] extraEscapeChars;

    /** The character used for escaping quotes. */
    public static final char DEFAULT_ESCAPE_CHARACTER = '"';

    /** The default separator to use if none is supplied to the constructor. */
    public static final char DEFAULT_SEPARATOR = ',';

    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_QUOTE_CHARACTER = '"';

    /** The quote constant to use when you wish to suppress all quoting. */
    public static final char NO_QUOTE_CHARACTER = '\u0000';

    /** The escape constant to use when you wish to suppress all escaping. */
    public static final char NO_ESCAPE_CHARACTER = '\u0000';

    /** Default line terminator uses platform encoding. */
    public static final String DEFAULT_LINE_END = "\n";

    /** Default value for quoting all value */
    public static final boolean DEFAULT_QUOTE_ALL = true;

    /** FLAG results returned by {@link #stringContainsSpecialCharacters(String)} */
    private static final byte NEEDS_QUOTE_FLAG = 0x01;
    private static final byte NEEDS_ESCAPE_FLAG = 0x02;

    private ResultSetHelper resultService = new ResultSetHelperService();

    /**
     * Constructs CSVWriter using a comma for the separator.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     */
    public CSVWriter(Writer writer) {
        this(writer, DEFAULT_SEPARATOR);
    }

    /**
     * Constructs CSVWriter with supplied separator.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries.
     */
    public CSVWriter(Writer writer, char separator) {
        this(writer, separator, DEFAULT_QUOTE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     */
    public CSVWriter(Writer writer, char separator, char quotechar) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escapechar
     *            the character to use for escaping quotechars or escapechars
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar) {
        this(writer, separator, quotechar, escapechar, DEFAULT_LINE_END);
    }


    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param lineEnd
     *            the line feed terminator to use
     */
    public CSVWriter(Writer writer, char separator, char quotechar, String lineEnd) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER, lineEnd);
    }



    /**
     * Constructs CSVWriter with supplied separator, quote char, escape char and line ending.
     *
     * @param writer
     *            the writer to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escapechar
     *            the character to use for escaping quotechars or escapechars
     * @param lineEnd
     *            the line feed terminator to use
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd) {
        this(writer, separator, quotechar, escapechar, lineEnd, DEFAULT_QUOTE_ALL);
    }

    /**
     * Constructs CSVWriter with supplied separator, quote char, escape char and line ending.
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     * @param lineEnd    the line feed terminator to use
     * @param quoteAll   if {@code true} all values in the csv will be quoted even if not needed
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd, boolean quoteAll) {
        this(writer, separator, quotechar, escapechar, lineEnd, quoteAll, null);
    }

    /**
     * Constructs CSVWriter with all supplied parameters
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     * @param lineEnd    the line feed terminator to use
     * @param quoteAll   if {@code true} all values in the csv will be quoted even if not needed
     * @param nullString a string for null fields or {@code null} if no null string
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd, boolean quoteAll,
            String nullString) {
        this.rawWriter = writer;
        this.pw = new PrintWriter(writer);
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
        this.lineEnd = lineEnd;
        this.quoteAll = quoteAll;
        this.nullString = nullString;
        validateNullString();
    }

    // TSV writer escaping carriage return and newline characters
    public static CSVWriter getStrictTSVWriter(Writer writer) {
        CSVWriter retval = new CSVWriter(writer, '\t', NO_QUOTE_CHARACTER, '\\', DEFAULT_LINE_END);
        retval.setEscapedNewlines();
        return retval;
    }

    // TSV writer escaping nothing
    public static CSVWriter getTSVWriter(Writer writer) {
        CSVWriter retval = new CSVWriter(writer, '\t', NO_QUOTE_CHARACTER, NO_ESCAPE_CHARACTER, DEFAULT_LINE_END);
        return retval;
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines
     *            a List of String[], with each String[] representing a line of
     *            the file.
     */
    public void writeAll(List<String[]> allLines)  {
        for (String[] line : allLines) {
            writeNext(line);
        }
    }

    public void setEscapedNewlines() {
        extraEscapeChars = new char[] { '\r', '\n' };
    }

    protected void writeColumnNames(ResultSet rs)
        throws SQLException {

        writeNext(resultService.getColumnNames(rs));
    }

    /**
     * Writes the entire ResultSet to a CSV file.
     *
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs the recordset to write
     * @param includeColumnNames true if you want column names in the output, false otherwise
     *
     * @throws java.io.IOException thrown by getColumnValue
     * @throws java.sql.SQLException thrown by getColumnValue
     */
    public void writeAll(java.sql.ResultSet rs, boolean includeColumnNames)  throws SQLException, IOException {
        if (includeColumnNames) {
            writeColumnNames(rs);
        }

        while (rs.next())
        {
            writeNext(resultService.getColumnValues(rs));
        }
    }


    /**
     * Writes the next line to the file.
     *
     * @param nextLine
     *            a string array with each comma-separated element as a separate
     *            entry.
     */
    public void writeNext(String[] nextLine) {
        if (nextLine == null) {
            return;
        }

        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int i = 0; i < nextLine.length; i++) {

            if (i != 0) {
                sb.append(separator);
            }

            String nextElement = nextLine[i];
            if (nextElement == null) {
                if (nullString != null) {
                    if (quoteAll && quotechar != NO_QUOTE_CHARACTER) {
                        sb.append(quotechar).append(nullString).append(quotechar);
                    } else {
                        sb.append(nullString);
                    }
                }
                continue;
            }

            byte searchResult = stringContainsSpecialCharacters(nextElement);

            if ((searchResult & NEEDS_QUOTE_FLAG) == NEEDS_QUOTE_FLAG) {
                sb.append(quotechar);
            }

            if ((searchResult & NEEDS_ESCAPE_FLAG) == NEEDS_ESCAPE_FLAG) {
                escapeElement(sb, nextElement);
            } else {
                sb.append(nextElement);
            }

            if ((searchResult & NEEDS_QUOTE_FLAG) == NEEDS_QUOTE_FLAG) {
                sb.append(quotechar);
            }
        }

        sb.append(lineEnd);
        pw.write(sb.toString());

    }

    /**
     * {@link #NEEDS_ESCAPE_FLAG} or {@link #NEEDS_QUOTE_FLAG} are only set if the element needs to be quoted or
     * escaped. The flags will not be set if the corresponding character is not set.
     *
     * @param element to test
     * @return flag result indicating if element needs to be quoted or escaped
     */
    private byte stringContainsSpecialCharacters(String element) {
        byte result = 0;
        if (quotechar == NO_QUOTE_CHARACTER && escapechar == NO_ESCAPE_CHARACTER) {
            // Cannot quote or escape so return
            return result;
        }

        if (quoteAll && quotechar != NO_QUOTE_CHARACTER) {
            // Always quote when quoteAll is true and there is a character to quote with
            result = NEEDS_QUOTE_FLAG;
        }

        int len = element.length();
        for (int i = 0; i < len; ++i) {
            char c = element.charAt(i);

            if (c == quotechar) {
                // quote chars always need to be quoted and escaped so no need to look further
                if (quotechar != NO_QUOTE_CHARACTER) {
                    result |= NEEDS_QUOTE_FLAG;
                }
                if (escapechar != NO_ESCAPE_CHARACTER) {
                    result |= NEEDS_ESCAPE_FLAG;
                }
                return result;
            }

            // Only need to quote if separator char or CRLF is in the string. See https://tools.ietf.org/html/rfc4180
            if (quotechar != NO_QUOTE_CHARACTER && (c == separator || c == '\n' || c == '\r')) {
                result |= NEEDS_QUOTE_FLAG;
            }

            if (escapechar != NO_ESCAPE_CHARACTER) {
                if (c == escapechar) {
                    result |= NEEDS_ESCAPE_FLAG;
                } else if (extraEscapeChars != null) {
                    for (char e : extraEscapeChars) {
                        if (c == e) {
                            result |= NEEDS_ESCAPE_FLAG;
                            break;
                        }
                    }
                }
            }

            // If the flag is set or there is no character for set then stop iteration
            if ((escapechar == NO_ESCAPE_CHARACTER || (result & NEEDS_ESCAPE_FLAG) == NEEDS_ESCAPE_FLAG)
                    && (quotechar == NO_QUOTE_CHARACTER || (result & NEEDS_QUOTE_FLAG) == NEEDS_QUOTE_FLAG)) {
                return result;
            }
        }

        return result;
    }

    protected void escapeElement(StringBuilder sb, String nextElement)
    {
        assert escapechar != NO_ESCAPE_CHARACTER;

        for (int j = 0; j < nextElement.length(); j++) {
            char nextChar = nextElement.charAt(j);
            if (nextChar == quotechar) {
                sb.append(escapechar).append(nextChar);
                continue;
            }
            if (nextChar == escapechar) {
                sb.append(escapechar).append(nextChar);
                continue;
            }
            if (extraEscapeChars != null) {
                boolean matched = false;
                for (char eec : extraEscapeChars) {
                    if (nextChar == eec) {
                        sb.append(escapechar).append(nextChar);
                        matched = true;
                        break;
                    }
                }
                if (matched) {
                    continue;
                }
            }
            // else not escaped
            sb.append(nextChar);
        }
    }

    /**
     * Flush underlying stream to writer.
     *
     * @throws IOException if bad things happen
     */
    public void flush() throws IOException {

        pw.flush();

    }

    /**
     * Close the underlying stream writer flushing any buffered content.
     *
     * @throws IOException if bad things happen
     *
     */
    @Override
    public void close() throws IOException {
        flush();
        pw.close();
        rawWriter.close();
    }

    /**
     *  Checks to see if the there has been an error in the printstream.
     */
    public boolean checkError() {
        return pw.checkError();
    }

    public void setResultService(ResultSetHelper resultService) {
        this.resultService = resultService;
    }

    // A VoltDB extension to support reset PrintWriter
    public void resetWriter() {
        pw = new PrintWriter(rawWriter);
    }

    private void validateNullString() {
        if (nullString == null) return;

        // Reject separator and quote character in null string
        // (we accept the escape char to support the default null string "\N")
        int len = nullString.length();
        for (int i = 0; i < len; ++i) {
            char c = nullString.charAt(i);

            if (c == separator || (c == quotechar && quotechar != NO_QUOTE_CHARACTER)) {
                throw new IllegalArgumentException("CSV null string cannot contain separator or quote");
            }

            // FIXME: should we also reject the characters in extraEscapeChars
        }
    }
    // End of VoltDB extension
}
