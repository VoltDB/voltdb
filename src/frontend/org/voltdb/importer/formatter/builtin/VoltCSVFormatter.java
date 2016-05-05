/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.io.StringReader;
import java.util.List;
import java.util.Properties;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv_voltpatches.tokenizer.Tokenizer;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

public class VoltCSVFormatter implements Formatter<String> {

    /** String that can be used to indicate NULL value in CSV files */
    public static final String CSV_NULL = "\\N";

    /** String that can be used to indicate NULL value in CSV files */
    public static final String QUOTED_CSV_NULL = "\"\\N\"";

    /**
     * Size limit for each column.
     */
    public static final long DEFAULT_COLUMN_LIMIT_SIZE = 16777216;

    private CsvPreference m_csvPreference;
    private String m_blank;
    private String m_customNullString;
    private boolean m_nowhitespace;
    private boolean m_surroundingSpacesNeedQuotes;
    private char m_separator;
    private char m_escape;
    private boolean m_strictquotes;

    VoltCSVFormatter(String formatName, Properties prop) {

        if (!("csv".equalsIgnoreCase(formatName) || "tsv".equalsIgnoreCase(formatName))) {
            throw new IllegalArgumentException(
                    "Invalid format " + formatName + ", choices are either \"csv\" or \"tsv\".");
        }

        m_separator = "csv".equalsIgnoreCase(formatName) ? ',' : '\t';

        String separatorProp = prop.getProperty("separator", "");
        if (!separatorProp.isEmpty() && separatorProp.length() == 1) {
            m_separator = separatorProp.charAt(0);
        }

        char quotechar = '"';
        String quoteCharProp = prop.getProperty("quotechar", "");
        if (!quoteCharProp.isEmpty() && quoteCharProp.length() == 1) {
            quotechar = quoteCharProp.charAt(0);
        }

        m_escape = '\\';
        String escapeProp = prop.getProperty("escape", "");
        if (!escapeProp.isEmpty() && escapeProp.length() == 1) {
            m_escape = escapeProp.charAt(0);
        }

        m_strictquotes = false;
        String strictQuotesProp = prop.getProperty("strictquotes", "");
        if (!strictQuotesProp.isEmpty()) {
            m_strictquotes = Boolean.parseBoolean(strictQuotesProp);
        }

        m_surroundingSpacesNeedQuotes = false;
        String surroundingSpacesNeedQuotes = prop.getProperty("surroundingSpacesNeedQuotes", "");
        if (!surroundingSpacesNeedQuotes.isEmpty()) {
            m_surroundingSpacesNeedQuotes = Boolean.parseBoolean(surroundingSpacesNeedQuotes);
        }

        m_blank = prop.getProperty("blank", "");

        m_customNullString = prop.getProperty("customNullString", "");
        if (!m_customNullString.isEmpty() && !"error".equals(m_blank)) {
            m_blank = "empty";
        }

        m_nowhitespace = false;
        String ignoreWhiteSpaceProp = prop.getProperty("nowhitespace", "");
        if (!ignoreWhiteSpaceProp.isEmpty()) {
            m_nowhitespace = Boolean.parseBoolean(ignoreWhiteSpaceProp);
        }

        CsvPreference.Builder builder = new CsvPreference.Builder(quotechar, m_separator, "\n");
        if (m_surroundingSpacesNeedQuotes) {
            builder.surroundingSpacesNeedQuotes(true);
        }
        m_csvPreference = builder.build();
    }

    @Override
    public Object[] transform(String sourceData) throws FormatException {

        if (sourceData == null) {
            return null;
        }

        Tokenizer tokenizer = new Tokenizer(new StringReader(sourceData), m_csvPreference, m_strictquotes, m_escape,
                DEFAULT_COLUMN_LIMIT_SIZE, 0);

        CsvListReader csvReader = new CsvListReader(tokenizer, m_csvPreference);

        List<String> dataList;
        try {
            dataList = csvReader.read();
        } catch (IOException e) {
            throw new FormatException("Fail to parse csv data", e);
        } finally {
            try {
                if (csvReader != null)
                    csvReader.close();
            } catch (IOException e) {
                throw new FormatException(e);
            }
        }
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
                    throw new FormatException("Whitespace detected--nowhitespace is used");
                } else {
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
}
