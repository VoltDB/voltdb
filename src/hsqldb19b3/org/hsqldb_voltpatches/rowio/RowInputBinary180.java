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


package org.hsqldb_voltpatches.rowio;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public class RowInputBinary180 extends RowInputBinary {

    Calendar tempCalDefault = new GregorianCalendar();

    public RowInputBinary180(byte[] buf) {
        super(buf);
    }

    protected TimeData readTime(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIME) {
            long millis = readLong();

            millis = HsqlDateTime.convertMillisFromCalendar(
                tempCalDefault, millis);
            millis = HsqlDateTime.getNormalisedTime(millis);

            return new TimeData((int) (millis / 1000), 0, 0);
        } else {
            return new TimeData(readInt(), readInt(), readInt());
        }
    }

    protected TimestampData readDate(Type type) throws IOException {

        long millis = readLong();

        millis = HsqlDateTime.convertMillisFromCalendar(tempCalDefault,
                                               millis);

        millis = HsqlDateTime.getNormalisedDate(millis);

        return new TimestampData(millis / 1000);
    }

    protected TimestampData readTimestamp(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIMESTAMP) {
            long millis = readLong();
            int  nanos  = readInt();

            millis = HsqlDateTime.convertMillisFromCalendar(tempCalDefault,
                                                   millis);

            return new TimestampData(millis / 1000, nanos);
        } else {
            return new TimestampData(readLong(), readInt(), readInt());
        }
    }
}
