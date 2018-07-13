/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb.utils;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.types.TimestampType;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;

import static org.voltdb.common.Constants.ODBC_DATE_FORMAT_STRING;

public class JavaBuildinFunctions {
    public int constantIntFunction() {
        return 0;
    }
    public Integer generalIntFunction(int arg0, Integer arg1) {
        return 2;
    }
    public String format_timestamp(TimestampType ts, String tz) throws VoltAbortException {
        String tmp = ODBC_DATE_FORMAT_STRING;
        long millis = ts.getTime()%1000;
        short usecs = ts.getUSec();
        Instant fromEpochMilli = Instant.ofEpochMilli(millis);
//        throw new VoltAbortException("Unrecognized selector.");
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(fromEpochMilli, ZoneId.of(tz));
        int offsetInSeconds = dateTime.getOffset().getTotalSeconds();

        // if invalid??
//        ZonedDateTime toDateTime = fromDateTime.withZoneSameInstant(ZoneId.of(tz));
//        millis=toDateTime.toInstant().toEpochMilli();
//        ZonedDateTime DateTime = ZonedDateTime.ofInstant(fromEpochMilli, ZoneId.of(tz));
//        millis=DateTime.toInstant().toEpochMilli();
        TimestampType convertedTs = new TimestampType( millis*1000 + usecs + offsetInSeconds*1000000);
        return convertedTs.toString();
    }
}
