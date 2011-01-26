/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

public class CSVEscaperUtil
{
    private static final char[] m_tsvEscapeChars = new char[] { '\t', '\n', '\r', '\\' };
    private static final char m_tsvDelimiter = '\t';
    private static final String m_tsvExtension = "tsv";
    private static final char[] m_csvEscapeChars = new char[] { ',', '"', '\n', '\r' };
    private static final char m_csvDelimiter = ',';
    private static final String m_csvExtension = "csv";

    public interface Escaper {
        public char getDelimiter();
        public String getExtension();
        public String escape(String s);
    }

    private static boolean contains(final String s, final char characters[]) {
        for (int ii = 0; ii < s.length(); ii++) {
            char c = s.charAt(ii);
            for (int qq = 0; qq < characters.length; qq++) {
                if (characters[qq] == c) {
                    return true;
                }
            }
        }
        return false;
    }

    public static final class CSVEscaper implements Escaper
    {
        public char getDelimiter()
        {
            return m_csvDelimiter;
        }

        public String getExtension()
        {
            return m_csvExtension;
        }

        public String escape(String s) {
            if (!contains(s, m_csvEscapeChars)) {
                return s;
            }
            StringBuilder sb = new StringBuilder(s.length() + (int)(s.length() * .10));
            sb.append('"');
            for (int ii = 0; ii < s.length(); ii++) {
                char c = s.charAt(ii);
                if (c == '"') {
                    sb.append("\"\"");
                } else {
                    sb.append(c);
                }
            }
            sb.append('"');
            return sb.toString();
        }
    }
    public static final class TSVEscaper implements Escaper
    {
        public char getDelimiter()
        {
            return m_tsvDelimiter;
        }

        public String getExtension()
        {
            return m_tsvExtension;
        }

        public String escape(String s) {
            if (!contains( s, m_tsvEscapeChars)) {
                return s;
            }
            StringBuilder sb = new StringBuilder(s.length() + (int)(s.length() * .10));
            for (int ii = 0; ii < s.length(); ii++) {
                char c = s.charAt(ii);
                if (c == '\\') {
                    sb.append("\\\\");
                } else if(c == '\t') {
                    sb.append("\\t");
                } else if (c == '\n') {
                    sb.append("\\n");
                } else if (c == '\r') {
                    sb.append("\\r");
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }
    }
}
