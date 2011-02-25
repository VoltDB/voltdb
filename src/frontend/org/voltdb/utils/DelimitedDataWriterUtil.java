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

import java.io.Writer;

public class DelimitedDataWriterUtil {
    private static final String m_tsvEscapeMatch = "(?i).*[\t\r\n\\\\].*";
    private static final char m_tsvDelimiter = '\t';
    private static final String m_tsvExtension = "tsv";
    private static final String m_csvEscapeMatch = "(?i).*[\r\n\",].*";
    private static final char m_csvDelimiter = ',';
    private static final String m_csvExtension = "csv";

    public interface DelimitedDataWriter {
        public String getExtension();

        public void writeRawField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException;

        public void writeEscapedField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException;

        public void writeRawField(StringBuilder writer, String s,
                boolean prependDelimiter);

        public void writeEscapedField(StringBuilder writer, String s,
                boolean prependDelimiter);
    }

    public static final class CSVWriter implements DelimitedDataWriter {
        public String getExtension() {
            return m_csvExtension;
        }

        public void writeRawField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException {
            if (prependDelimiter)
                writer.write(m_csvDelimiter);
            writer.write(s);
        }

        public void writeRawField(StringBuilder writer, String s,
                boolean prependDelimiter) {
            if (prependDelimiter)
                writer.append(m_csvDelimiter);
            writer.append(s);
        }

        public void writeEscapedField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException {
            if (prependDelimiter)
                writer.write(m_csvDelimiter);
            if (s.matches(m_csvEscapeMatch)) {
                writer.write("\"");
                for (int ii = 0; ii < s.length(); ii++) {
                    char c = s.charAt(ii);
                    switch (c) {
                    case '"':
                        writer.write("\"\"");
                        break;
                    default:
                        writer.write(c);
                        break;
                    }
                }
                writer.write("\"");
            } else {
                writer.write(s);
            }
        }

        public void writeEscapedField(StringBuilder writer, String s,
                boolean prependDelimiter) {
            if (prependDelimiter)
                writer.append(m_csvDelimiter);
            if (s.matches(m_csvEscapeMatch)) {
                writer.append("\"");
                for (int ii = 0; ii < s.length(); ii++) {
                    char c = s.charAt(ii);
                    switch (c) {
                    case '"':
                        writer.append("\"\"");
                        break;
                    default:
                        writer.append(c);
                        break;
                    }
                }
                writer.append("\"");
            } else {
                writer.append(s);
            }
        }
    }

    public static final class TSVWriter implements DelimitedDataWriter {
        public String getExtension() {
            return m_tsvExtension;
        }

        public void writeRawField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException {
            if (prependDelimiter)
                writer.write(m_tsvDelimiter);
            writer.write(s);
        }

        public void writeRawField(StringBuilder writer, String s,
                boolean prependDelimiter) {
            if (prependDelimiter)
                writer.append(m_tsvDelimiter);
            writer.append(s);
        }

        public void writeEscapedField(Writer writer, String s,
                boolean prependDelimiter) throws java.io.IOException {
            if (prependDelimiter)
                writer.write(m_tsvDelimiter);
            if (s.matches(m_tsvEscapeMatch)) {
                for (int ii = 0; ii < s.length(); ii++) {
                    char c = s.charAt(ii);
                    switch (c) {
                    case '\\':
                        writer.write("\\\\");
                        break;
                    case '\t':
                        writer.write("\\t");
                        break;
                    case '\r':
                        writer.write("\\r");
                        break;
                    case '\n':
                        writer.write("\\n");
                        break;
                    default:
                        writer.write(c);
                        break;
                    }
                }
            } else {
                writer.write(s);
            }
        }

        public void writeEscapedField(StringBuilder writer, String s,
                boolean prependDelimiter) {
            if (prependDelimiter)
                writer.append(m_tsvDelimiter);
            if (s.matches(m_tsvEscapeMatch)) {
                for (int ii = 0; ii < s.length(); ii++) {
                    char c = s.charAt(ii);
                    switch (c) {
                    case '\\':
                        writer.append("\\\\");
                        break;
                    case '\t':
                        writer.append("\\t");
                        break;
                    case '\r':
                        writer.append("\\r");
                        break;
                    case '\n':
                        writer.append("\\n");
                        break;
                    default:
                        writer.append(c);
                        break;
                    }
                }
            } else {
                writer.append(s);
            }
        }
    }
}
