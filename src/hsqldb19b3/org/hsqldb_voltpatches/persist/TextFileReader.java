/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.persist;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowInputText;

/**
 * Reader for text files.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.2.7
*/
public class TextFileReader {

    private RandomAccessInterface     dataFile;
    private RowInputInterface         rowIn;
    private TextFileSettings          textFileSettings;
    private String                    header;
    private boolean                   isReadOnly;
    private HsqlByteArrayOutputStream buffer;

    TextFileReader(RandomAccessInterface dataFile,
                   TextFileSettings textFileSettings, RowInputInterface rowIn,
                   boolean isReadOnly) {

        this.dataFile         = dataFile;
        this.textFileSettings = textFileSettings;
        this.rowIn            = rowIn;
        this.isReadOnly       = isReadOnly;
        this.buffer           = new HsqlByteArrayOutputStream(128);
    }

    public RowInputInterface readObject(long pos) {

        boolean hasQuote  = false;
        boolean complete  = false;
        boolean wasCR     = false;
        boolean wasNormal = false;

        buffer.reset();

        pos = findNextUsedLinePos(pos);

        if (pos == -1) {
            return null;
        }

        try {
            dataFile.seek(pos);

            while (!complete) {
                int c = dataFile.read();

                wasNormal = false;

                if (c == -1) {
                    if (buffer.size() == 0) {
                        return null;
                    }

                    complete = true;

                    if (wasCR) {
                        break;
                    }

                    if (!isReadOnly) {
                        dataFile.write(TextFileSettings.BYTES_LINE_SEP, 0,
                                       TextFileSettings.BYTES_LINE_SEP.length);
                        buffer.write(TextFileSettings.BYTES_LINE_SEP);
                    }

                    break;
                }

                switch (c) {

                    case TextFileSettings.DOUBLE_QUOTE_CHAR :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;

                        if (textFileSettings.isQuoted) {
                            hasQuote = !hasQuote;
                        }
                        break;

                    case TextFileSettings.CR_CHAR :
                        wasCR = !hasQuote;
                        break;

                    case TextFileSettings.LF_CHAR :
                        complete = !hasQuote;
                        break;

                    default :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;
                }

                buffer.write(c);
            }

            if (complete) {
                if (wasNormal) {
                    buffer.setPosition(buffer.size() - 1);
                }

                String rowString;

                try {
                    rowString =
                        buffer.toString(textFileSettings.stringEncoding);
                } catch (UnsupportedEncodingException e) {
                    rowString = buffer.toString();
                }

                ((RowInputText) rowIn).setSource(rowString, pos,
                                                 buffer.size());

                return rowIn;
            }

            return null;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public int readHeaderLine() {

        boolean complete  = false;
        boolean wasCR     = false;
        boolean wasNormal = false;

        buffer.reset();

        try {
            dataFile.seek(0);
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }

        while (!complete) {
            wasNormal = false;

            int c;

            try {
                c = dataFile.read();

                if (c == -1) {
                    if (buffer.size() == 0) {
                        return 0;
                    }

                    complete = true;

                    if (!isReadOnly) {
                        dataFile.write(TextFileSettings.BYTES_LINE_SEP, 0,
                                       TextFileSettings.BYTES_LINE_SEP.length);
                        buffer.write(TextFileSettings.BYTES_LINE_SEP);
                    }

                    break;
                }
            } catch (IOException e) {
                throw Error.error(ErrorCode.TEXT_FILE);
            }

            switch (c) {

                case TextFileSettings.CR_CHAR :
                    wasCR = true;
                    break;

                case TextFileSettings.LF_CHAR :
                    complete = true;
                    break;

                default :
                    wasNormal = true;
                    complete  = wasCR;
                    wasCR     = false;
            }

            if (wasCR || complete) {
                continue;
            }

            buffer.write(c);
        }

        if (wasNormal) {
            buffer.setPosition(buffer.size() - 1);
        }

        try {
            header = buffer.toString(textFileSettings.stringEncoding);
        } catch (UnsupportedEncodingException e) {
            header = buffer.toString();
        }

        return buffer.size();
    }

    // fredt - new method

    /**
     * Searches from file pointer, pos, and finds the beginning of the first
     * line that contains any non-space character. Increments the row counter
     * when a blank line is skipped.
     *
     * If none found return -1
     */
    private long findNextUsedLinePos(long pos) {

        try {
            long     firstPos   = pos;
            long     currentPos = pos;
            boolean wasCR      = false;

            dataFile.seek(pos);

            while (true) {
                int c = dataFile.read();

                currentPos++;

                switch (c) {

                    case TextFileSettings.CR_CHAR :
                        wasCR = true;
                        break;

                    case TextFileSettings.LF_CHAR :
                        wasCR = false;

                        ((RowInputText) rowIn).skippedLine();

                        firstPos = currentPos;
                        break;

                    case ' ' :
                        if (wasCR) {
                            wasCR = false;

                            ((RowInputText) rowIn).skippedLine();
                        }
                        break;

                    case -1 :
                        return -1;

                    default :
                        if (wasCR) {
                            wasCR = false;

                            ((RowInputText) rowIn).skippedLine();
                        }

                        return firstPos;
                }
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public String getHeaderLine() {
        return header;
    }

    public int getLineNumber() {
        return ((RowInputText) rowIn).getLineNumber();
    }
}
