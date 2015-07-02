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
public class RowOutputBinary180 extends RowOutputBinary {

    Calendar tempCalDefault = new GregorianCalendar();

    public RowOutputBinary180(int initialSize, int scale) {
        super(initialSize, scale);
    }

    protected void writeDate(TimestampData o, Type type) {

        long millis = o.getSeconds() * 1000L;

        millis = HsqlDateTime.convertMillisToCalendar(tempCalDefault, millis);

        writeLong(millis);
    }

    protected void writeTime(TimeData o, Type type) {

        if (type.typeCode == Types.SQL_TIME) {
            long millis = o.getSeconds() * 1000L;

            millis = HsqlDateTime.convertMillisToCalendar(tempCalDefault,
                    millis);

            writeLong(millis);
        } else {
            writeInt(o.getSeconds());
            writeInt(o.getNanos());
            writeInt(o.getZone());
        }
    }

    protected void writeTimestamp(TimestampData o, Type type) {

        if (type.typeCode == Types.SQL_TIMESTAMP) {
            long millis = o.getSeconds() * 1000L;

            millis = HsqlDateTime.convertMillisToCalendar(tempCalDefault,
                    millis);

            writeLong(millis);
            writeInt(o.getNanos());
        } else {
            writeLong(o.getSeconds());
            writeInt(o.getNanos());
            writeInt(o.getZone());
        }
    }
}
