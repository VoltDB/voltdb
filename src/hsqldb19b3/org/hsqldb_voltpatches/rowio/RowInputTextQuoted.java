/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.rowio;

import java.io.IOException;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;

/**
 * Fields in the source file need not be quoted. Methods in this class unquote
 * the fields if they are quoted and handle quote character doubling in this
 * case.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class RowInputTextQuoted extends RowInputText {

    private static final int NORMAL_FIELD   = 0;
    private static final int NEED_END_QUOTE = 1;
    private static final int FOUND_QUOTE    = 2;
    private char[]           qtext;

    public RowInputTextQuoted(String fieldSep, String varSep,
                              String longvarSep, boolean allQuoted) {
        super(fieldSep, varSep, longvarSep, allQuoted);
    }

    public void setSource(String text, int pos, int byteSize) {

        super.setSource(text, pos, byteSize);

        qtext = text.toCharArray();
    }

    protected String getField(String sep, int sepLen,
                              boolean isEnd) throws IOException {

        //fredt - now the only supported behaviour is emptyIsNull
        String s = null;

        if (next >= qtext.length || qtext[next] != '\"') {
            return super.getField(sep, sepLen, isEnd);
        }

        try {
            field++;

            StringBuffer sb    = new StringBuffer();
            boolean      done  = false;
            int          state = NORMAL_FIELD;
            int          end   = -1;

            if (!isEnd) {
                end = text.indexOf(sep, next);
            }

            for (; next < qtext.length; next++) {
                switch (state) {

                    case NORMAL_FIELD :
                    default :
                        if (next == end) {
                            next += sepLen;
                            done = true;
                        } else if (qtext[next] == '\"') {

                            //-- Beginning of field
                            state = NEED_END_QUOTE;
                        } else {
                            sb.append(qtext[next]);
                        }
                        break;

                    case NEED_END_QUOTE :
                        if (qtext[next] == '\"') {
                            state = FOUND_QUOTE;
                        } else {
                            sb.append(qtext[next]);
                        }
                        break;

                    case FOUND_QUOTE :
                        if (qtext[next] == '\"') {

                            //-- Escaped quote
                            sb.append(qtext[next]);

                            state = NEED_END_QUOTE;
                        } else {
                            next  += sepLen - 1;
                            state = NORMAL_FIELD;

                            if (!isEnd) {
                                next++;

                                done = true;
                            }
                        }
                        break;
                }

                if (done) {
                    break;
                }
            }

            s = sb.toString();
        } catch (Exception e) {
            throw new IOException(
                Error.getMessage(
                    ErrorCode.M_TEXT_SOURCE_FIELD_ERROR, 0, new Object[] {
                new Integer(field), e.toString()
            }));
        }

        return s;
    }
}
