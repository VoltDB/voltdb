/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importer.formatter.builtin;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv_voltpatches.tokenizer.Tokenizer;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

public class VoltSuperCSVFormatter implements Formatter {

    /** String that can be used to indicate NULL value in CSV files */
    public static final String CSV_NULL = "\\N";

    /** String that can be used to indicate NULL value in CSV files */
    public static final String QUOTED_CSV_NULL = "\"\\N\"";

    public static final char DEFAULT_QUOTE_CHAR = '"';

    public static final char DEFAULT_ESCAPE_CHAR = '\\';

    /**
     * Size limit for each column.
     */
    public static final long DEFAULT_COLUMN_LIMIT_SIZE = 16777216;

    private String m_blank;
    private String m_customNullString;
    private boolean m_nowhitespace;
    private boolean m_surroundingSpacesNeedQuotes;
    private char m_separator;
    private char m_escape;
    private boolean m_strictquotes;
    private VoltCVSTokenizer m_tokenizer;
    CsvListReader m_csvReader;

    public VoltSuperCSVFormatter(String formatName, Properties prop) {

        if (!("csv".equalsIgnoreCase(formatName) || "tsv".equalsIgnoreCase(formatName))) {
            throw new IllegalArgumentException(
                    "Invalid format " + formatName + ", expected \"csv\" or \"tsv\".");
        }

        m_separator = "csv".equalsIgnoreCase(formatName) ? ',' : '\t';

        String separatorProp = prop.getProperty("separator", "").trim();
        if (!separatorProp.isEmpty() && separatorProp.length() == 1) {
            m_separator = separatorProp.charAt(0);
        }

        char quotechar = DEFAULT_QUOTE_CHAR;
        String quoteCharProp = prop.getProperty("quotechar", "").trim();
        if (!quoteCharProp.isEmpty() && quoteCharProp.length() == 1) {
            quotechar = quoteCharProp.charAt(0);
        }

        m_escape = DEFAULT_ESCAPE_CHAR;
        String escapeProp = prop.getProperty("escape", "").trim();
        if (!escapeProp.isEmpty() && escapeProp.length() == 1) {
            m_escape = escapeProp.charAt(0);
        }

        m_strictquotes = "true".equalsIgnoreCase(prop.getProperty("strictquotes", ""));
        m_surroundingSpacesNeedQuotes = "true".equalsIgnoreCase(prop.getProperty("trimunquoted", ""));
        m_blank = prop.getProperty("blank", "").trim();

        m_customNullString = prop.getProperty("nullstring", "").trim();
        if (!m_customNullString.isEmpty() && !"error".equals(m_blank)) {
            m_blank = "empty";
        }

        m_nowhitespace = "true".equalsIgnoreCase(prop.getProperty("nowhitespace", ""));

        CsvPreference.Builder builder = new CsvPreference.Builder(quotechar, m_separator, "\n");
        if (m_surroundingSpacesNeedQuotes) {
            builder.surroundingSpacesNeedQuotes(true);
        }
        CsvPreference csvPreference = builder.build();

        m_tokenizer = new VoltCVSTokenizer(new StringReader(""), csvPreference, m_strictquotes, m_escape,
                DEFAULT_COLUMN_LIMIT_SIZE, 0);

        m_csvReader = new CsvListReader(m_tokenizer, csvPreference);
    }

    @Override
    public Object[] transform(ByteBuffer payload) throws FormatException {
        if (payload == null) {
            return null;
        }
        String line = new String(payload.array(), payload.arrayOffset(), payload.limit(), StandardCharsets.UTF_8);
        m_tokenizer.setSourceString(line);
        List<String> dataList;
        try {
            dataList = m_csvReader.read();
        } catch (IOException | SuperCsvException e) {
            throw new FormatException("Failed to parse csv data", e);
        }
        if (dataList == null) return null;
        String[] data = dataList.toArray(new String[0]);
        normalize(data);
        return data;
    }

    private void normalize(String[] lineValues) throws FormatException {

        for (int i = 0; i < lineValues.length; i++) {

            if (lineValues[i] == null) {
                if ("error".equals(m_blank)) {
                    throw new FormatException("Blank values are not allowed");
                }
            } else {
                if (m_nowhitespace && (lineValues[i].charAt(0) == ' '
                        || lineValues[i].charAt(lineValues[i].length() - 1) == ' ')) {
                    throw new FormatException("Whitespace detectet when nowhitespace is used");
                } else if (m_surroundingSpacesNeedQuotes) {
                    lineValues[i] = lineValues[i].trim();
                }

                if (!m_customNullString.isEmpty()) {
                    if (m_customNullString.equals(lineValues[i])) {
                        lineValues[i] = null;
                    }
                } else if ("NULL".equals(lineValues[i]) || CSV_NULL.equalsIgnoreCase(lineValues[i])
                        || QUOTED_CSV_NULL.equals(lineValues[i])) {
                    lineValues[i] = null;
                }
            }
        }
    }

    /**
     * Importers transform the source one row at time. VoltCVSTokenizer will cut significant amount time on processing data
     * via reader and cell processor and improve the performance.
     *
     */
    private class VoltCVSTokenizer extends Tokenizer {

        private String m_sourceString;

        public VoltCVSTokenizer(Reader reader, CsvPreference preferences, boolean strictquotes, char escapechar,
                long columnsizelimit, long skipNum) {
            super(reader, preferences, strictquotes, escapechar, columnsizelimit, skipNum);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String readLine() throws IOException {

            String tempStr = m_sourceString;

            //set to null to mark EOF
            m_sourceString = null;

            return tempStr;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getLineNumber() {
            return 1;
        }

        public void setSourceString(String sourceString) {
            m_sourceString = sourceString;
        }
    }
}
