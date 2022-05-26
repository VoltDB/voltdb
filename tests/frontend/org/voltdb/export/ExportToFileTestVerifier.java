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
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Preconditions;
import org.voltdb.exportclient.ExportDecoderBase;

public class ExportToFileTestVerifier {
    private final ArrayDeque<String[]> m_data = new ArrayDeque<String[]>();
    private int m_sequenceNumber = 0;
    protected final ThreadLocal<SimpleDateFormat> m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        }
    };

    private final int partitionId;

    public ExportToFileTestVerifier(int partitionId) {
        this.partitionId = partitionId;
    }

    void addRow( String [] data) {
        Preconditions.checkArgument(
                data == null || data.length > 0,
                "row size does not match expected row size"
                );
        m_data.offer(data);
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
            } else if (cval instanceof String) {
                row[i] = (String)cval;
            } else if (cval instanceof TimestampType) {
                row[i] = m_ODBCDateformat.get().format(
                        ((TimestampType)cval).asApproximateJavaDate()
                        );
            } else {
                row[i] = cval.toString();
            }
        }
        m_data.offer(row);
    }

    Matcher<String[]> isExpectedRow() {
        return new TypeSafeDiagnosingMatcher<String[]>() {
            String [] expected = ( m_data.peek() == null ? null : m_data.poll() );
            Matcher<Integer> seqMatcher = equalTo(m_sequenceNumber);

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
                if (match) {
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
                   String [] toBeMatched = Arrays.copyOfRange(
                           gotten, ExportDecoderBase.INTERNAL_FIELD_COUNT - 1,
                           gotten.length
                           );
                   Matcher<String[]> rowMatcher = arrayContaining(expected);
                   if( ! (match = rowMatcher.matches(toBeMatched))) {
                       rowMatcher.describeMismatch(toBeMatched, d);
                   }
                }
                d.appendText("]");
                System.out.println("Validated partition id " + partitionId + " sequence " + m_sequenceNumber);
                return match;
            }
        };
    }
}
