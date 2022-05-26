/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.export;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Arrays;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Preconditions;

public class ExportToSocketTestVerifier {
    private final ArrayDeque<String[]> m_data = new ArrayDeque<String[]>();
    private int m_totalCount = 0;
    private int m_sequenceNumber = 1;
    // Debug flag to enable verbose output
    private static boolean ENABLE_DEBUG = false;
    protected final ThreadLocal<SimpleDateFormat> m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        }
    };

    private final int m_partitionId;
    private final String m_tableName;

    public ExportToSocketTestVerifier(String tableName, int partitionId) {
        this.m_tableName = tableName;
        this.m_partitionId = partitionId;
    }

    void addRow( Object [] data) {
        Preconditions.checkArgument(data != null && data.length > 0, "row without column data");
        String [] row = new String[data.length];
        for (int i = 0; i < data.length; ++i) {
            Object cval = data[i];
            if (cval == null) {
                row[i] = "NULL";
            } else if (cval instanceof byte[]) {
                row[i] = Encoder.hexEncode((byte[])cval);
            } else if (cval instanceof TimestampType) {
                row[i] = m_ODBCDateformat.get().format(
                        ((TimestampType)cval).asApproximateJavaDate()
                        );
            } else {
                row[i] = cval.toString();
            }
        }
        if (ENABLE_DEBUG) {
            StringBuilder sb = new StringBuilder();
            for (String part : row) {
                sb.append(part).append(" ");
            }
            System.out.println("RowVerifier received on partition "
                    + m_partitionId + ":" + sb.toString());
        }
        m_data.offer(row);
        ++m_totalCount;
    }

    Matcher<String[]> isExpectedRow() {
        return isExpectedRow(true);
    }

    public Matcher<String[]> isExpectedRow(final boolean verifySequenceNumber) {
        return new TypeSafeDiagnosingMatcher<String[]>() {
            String [] expected = ( m_data.peek() == null ? null : m_data.poll() );
            int matchSequenceNumber = m_sequenceNumber;
            Matcher<Integer> seqMatcher = equalTo(matchSequenceNumber);

            @Override
            public void describeTo(Description d) {
                d.appendText("row [ {sequence ")
                 .appendValue(m_sequenceNumber)
                 .appendText("}");

                if (expected != null) {
                    d.appendValueList(", ", ", ", "", Arrays.<String>asList(expected));
                }

                d.appendText("]");
            }

            @Override
            protected boolean matchesSafely(String[] gotten, Description d) {
                d.appendText(" row [");
                boolean match = expected != null;
                if( ! match) {
                    d.appendText("{ EOD exhausted expected rows }");
                }
                if (match && verifySequenceNumber) {
                    int rowSeq = Integer.valueOf(gotten[2]);
                    if (! (match = seqMatcher.matches(rowSeq))) {
                        d.appendText("{ expected sequence " ).appendDescriptionOf(seqMatcher);
                        seqMatcher.describeMismatch(rowSeq, d);
                        d.appendText(" }");
                    } else {
                        m_sequenceNumber++;
                    }
                }
                if (match) {
                    String [] toBeMatched;
                    Matcher<String[]> rowMatcher;
                    if (verifySequenceNumber) {
                        toBeMatched = Arrays.copyOfRange(
                           gotten, ExportDecoderBase.INTERNAL_FIELD_COUNT - 1,
                           gotten.length
                           );
                        if (ENABLE_DEBUG) {
                            StringBuilder sb = new StringBuilder();
                            for (String matched : toBeMatched) {
                                sb.append(matched).append(" ");
                            }
                            StringBuilder anotherSb = new StringBuilder();
                            for (String expect: expected) {
                                anotherSb.append(expect).append(" ");
                            }
                            System.out.println("Comparing " + sb.toString() + "(received) to " + anotherSb.toString() + "(expected)");
                        }
                        rowMatcher = arrayContaining(expected);
                    } else {
                        toBeMatched = Arrays.copyOfRange(
                                gotten, ExportDecoderBase.INTERNAL_FIELD_COUNT + 1,
                                gotten.length
                                );
                        rowMatcher = arrayContaining(
                                Arrays.copyOfRange(
                                        expected, 2,
                                        expected.length
                                        ));
                    }
                    if( ! (match = rowMatcher.matches(toBeMatched))) {
                        rowMatcher.describeMismatch(toBeMatched, d);
                    }
                }
                d.appendText("]");
                if (ENABLE_DEBUG) {
                    System.out.println("Validated table " + m_tableName + " partition id " + m_partitionId + " sequence " + matchSequenceNumber);
                }
                return match;
            }
        };
    }

    public int getSize() {
        return m_data.size();
    }

    public int getTotalCount() {
        return m_totalCount;
    }
}
