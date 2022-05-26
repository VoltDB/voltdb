/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.ZoneId;

/**
 * This is the Java class that manages Java build-in function. It is the Java version of SQL functions,
 * but using the UDF mechanism.
 */
public class JavaBuiltInFunctions {
    public String format_timestamp(TimestampType ts, String tz) throws VoltAbortException {
        if (ts == null) {
            return null;
        }
        if (tz == null) {
            return ts.toString();
        }
        tz = tz.trim();
        long millis = ts.getTime() / 1000;
        short usecs = ts.getUSec();
        Instant fromEpochMilli = Instant.ofEpochMilli(millis);
        ZoneOffset offset;
        try {
            offset = ZonedDateTime.ofInstant(fromEpochMilli, ZoneId.of(tz)).getOffset();
        } catch (DateTimeException offsetEx) {
            throw new VoltAbortException("Invalid timezone string.");
        }

        long offsetInSeconds = offset.getTotalSeconds();

        TimestampType convertedTs = new TimestampType(millis * 1000 + usecs + offsetInSeconds * 1000000);
        return convertedTs.toString();
    }
}
