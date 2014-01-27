/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

/**
 * Utility class for writing numbers to an array of bytes.
 * Shortcut for when FastSerializer/Bytebuffer is overkill.
 *
 */
public class ByteArrayUtils {

    /**
     * Converts an 8 byte array of unsigned bytes to a singed long.
     * Note that this assumes a positive result.
     * @param b an array of 8 unsigned bytes
     * @return a long representing the 8 bytes
     */
    public static final long bytesToLong(byte[] buf, int offset)
    {
        long retval = 0;
        int i = 0;
        for (; i < 7; ++i) {
            retval |= buf[i + offset] & 0xFF;
            retval <<= 8;
        }
        retval |= buf[i + offset] & 0xFF;
        return retval;
    }

    /**
     * Converts a 4 byte array of unsigned bytes to a singed long.
     * Note that this assumes a positive result.
     * @param b an array of 4 unsigned bytes
     * @return an int representing the 4 bytes
     */
    public static final int bytesToInt(byte[] buf, int offset)
    {
        int retval = 0;
        int i = 0;
        for (; i < 3; ++i) {
            retval |= buf[i + offset] & 0xFF;
            retval <<= 8;
        }
        retval |= buf[i + offset] & 0xFF;
        return retval;
    }
}
