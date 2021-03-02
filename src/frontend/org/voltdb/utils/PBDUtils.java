/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

public class PBDUtils {
    public static class ConfigurationException extends Exception {
        private static final long serialVersionUID = 1L;
        ConfigurationException() { super(); }
        ConfigurationException(String s) { super(s); }
    }

    private static long s_minBytesLimitMb = 64;

    // A map of time configuration qualifiers to millisecond value
    private static final Map<String, Long> s_timeLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("ss", 1000L);
        bldr.put("mn", 60_000L);
        bldr.put("hr", 60L * 60_000L);
        bldr.put("dy", 24L * 60L * 60_000L);
        bldr.put("wk", 7L * 24L * 60L * 60_000L);
        bldr.put("mo", 30L * 24L * 60L * 60_000L);
        bldr.put("yr", 365L * 24L * 60L * 60_000L);
        s_timeLimitConverter = bldr.build();
    }

    // A map of byte configuration qualifiers to bytes value
    private static final Map<String, Long> s_byteLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("mb", 1024L * 1024L);
        bldr.put("gb", 1024L * 1024L * 1024L);
        s_byteLimitConverter = bldr.build();
    }

    public static void writeBuffer(FileChannel fc, ByteBuffer buf, int startPos) throws IOException
    {
        int pos = startPos;
        while (buf.hasRemaining()) {
            pos += fc.write(buf, pos);
        }
    }

    public static void readBufferFully(FileChannel fc, ByteBuffer buf, int startPos) throws IOException
    {
        int pos = startPos;
        while (buf.hasRemaining()) {
            int read = fc.read(buf, pos);
            if (read == -1) {
                throw new EOFException();
            }
            pos += read;
        }
        buf.flip();
    }

    public static int calculateEntryCrc(CRC32 crc, ByteBuffer destBuf, int entryId, char flags) {
        crc.reset();
        crc.update(destBuf.remaining());
        crc.update(entryId);
        crc.update(flags);
        crc.update(destBuf);
        // the checksum here is really an unsigned int, store integer to save 4 bytes
        return (int) crc.getValue();
    }

    public static void writeEntryHeader(CRC32 crc, ByteBuffer headerBuf,
            ByteBuffer destBuf, int entryId, char flag) {
        int length = destBuf.remaining();
        headerBuf.putInt(calculateEntryCrc(crc, destBuf, entryId, flag));
        headerBuf.putInt(length);
        headerBuf.putInt(entryId);
        headerBuf.putChar(flag);
    }

    /**
     * Convert time limit string to milliseconds
     * acceptable limit qualifiers (case insensitive):
     * "mn" (minutes), "hr" (hours), "dy" (days), "wk" (weeks), "mo" (months), "yr" (years)
     * @param limitStr time limt such as 3mn, 3hr
     * @return time in millisecond
     * @throws ConfigurationException
     */
    public static long parseTimeValue(String limitStr) throws ConfigurationException {
        return parseLimit(limitStr, s_timeLimitConverter);
    }

    /**
     * Convert size limit string to number of bytes
     * acceptable limit qualifiers (case insensitive): "mb" (megabytes, minimum 64), "gb"
     * @param limitStr byte limit, such as 50mb
     * @return bytes
     * @throws ConfigurationException
     */
    public static long parseByteValue(String limitStr) throws ConfigurationException {
        long limit = parseLimit(limitStr, s_byteLimitConverter);
        long minLimit = s_minBytesLimitMb * s_byteLimitConverter.get("mb");
        if (limit < minLimit) {
            throw new ConfigurationException("Size-based limit must be > " + s_minBytesLimitMb + " mb");
        }
        return limit;
    }

    /**
     * Convert time limit string in second to millisecond
     * @param intervalStr time limit in second
     * @return milliseconds
     * @throws ConfigurationException
     */
    public static long parseTimeInterval(String intervalStr) throws ConfigurationException {
        if (StringUtils.isBlank(intervalStr)) {
            throw new ConfigurationException("empty time interval");
        }
        try {
            long limit = TimeUnit.SECONDS.toMillis(Integer.parseInt(intervalStr));
            if (limit < 0) {
                throw new ConfigurationException("Interval must be positive: " + intervalStr);
            }
            return limit;
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Interval must be an integer: " + e);
        }
    }

    // Parse a limit qualified by a 2-character qualifier and return its converted value
    private static long parseLimit(String limitStr, Map<String, Long> cvt) throws ConfigurationException {
        if (StringUtils.isEmpty(limitStr)) {
            throw new ConfigurationException("empty input");
        }
        String parse = limitStr.trim().toLowerCase();
        if (parse.length() <= 2) {
            throw new ConfigurationException("\"" + limitStr + "\" is not a valid input");
        }
        String qualifier = parse.substring(parse.length() - 2);
        if (!cvt.keySet().contains(qualifier)) {
            throw new ConfigurationException("\"" + qualifier + "\" is not a valid qualifier: "
                    + cvt.keySet() + " are the valid values");
        }
        String valStr = parse.substring(0, parse.length() - 2);
        long limit = 0;
        try {
            limit = Long.parseLong(valStr.trim());
            limit *= cvt.get(qualifier);
        }
        catch (NumberFormatException ex) {
            throw new ConfigurationException("Failed to parse\"" + limitStr + "\": " + ex);
        }
        if (limit <= 0) {
            throw new ConfigurationException("A limit must have a positive value");
        }
        return limit;
    }
}
