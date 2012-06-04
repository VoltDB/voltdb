/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.UnsupportedEncodingException;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.LogKeys;

/**
 * Class that maps object values to partitions. It's rather simple
 * really. It'll get more complicated if you give it time.
 */
public abstract class TheHashinator {
    static int catalogPartitionCount;
    private static final VoltLogger hostLogger = new VoltLogger("HOST");

    /**
     * Initialize TheHashinator
     */
    public static void initialize(int partitionCount) {
        catalogPartitionCount = partitionCount;
    }

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinateLong(long value) {
        int partitionCount = TheHashinator.catalogPartitionCount;

        // special case this hard to hash value to 0 (in both c++ and java)
        if (value == Long.MIN_VALUE) return 0;

        // hash the same way c++ does
        int index = (int)(value^(value>>>32));
        return java.lang.Math.abs(index % partitionCount);
    }

    /**
     * Given an Object value, pick a partition to store the data. Currently only String objects can be hashed.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinateString(Object value) {
        int partitionCount = TheHashinator.catalogPartitionCount;

        if (value instanceof String) {
            String string = (String) value;
            try {
                byte bytes[] = string.getBytes("UTF-8");
                int hashCode = 0;
                int offset = 0;
                for (int ii = 0; ii < bytes.length; ii++) {
                   hashCode = 31 * hashCode + bytes[offset++];
                }
                return java.lang.Math.abs(hashCode % partitionCount);
            } catch (UnsupportedEncodingException e) {
                hostLogger.l7dlog( Level.FATAL, LogKeys.host_TheHashinator_ExceptionHashingString.name(), new Object[] { string }, e);
                VoltDB.crashLocalVoltDB("No additional info.", false, e);
            }
        }
        hostLogger.l7dlog(Level.FATAL, LogKeys.host_TheHashinator_AttemptedToHashinateNonLongOrString.name(), new Object[] { value
                .getClass().getName() }, null);
        VoltDB.crashLocalVoltDB("No additional info.", false, null);
        return -1;
    }

    /**
     * Given an object, map it to a partition.
     * @param obj The object to be mapped to a partition.
     * @return The id of the partition desired.
     */
    public static int hashToPartition(Object obj) {
        int index = 0;
        if (obj == null || VoltType.isNullVoltType(obj))
        {
            index = 0;
        }
        else if (obj instanceof Long) {
            long value = ((Long) obj).longValue();
            index = hashinateLong(value);
        } else if (obj instanceof String) {
            index = hashinateString(obj);
        } else if (obj instanceof Integer) {
            long value = ((Integer)obj).intValue();
            index = hashinateLong(value);
        } else if (obj instanceof Short) {
            long value = ((Short)obj).shortValue();
            index = hashinateLong(value);
        } else if (obj instanceof Byte) {
            long value = ((Byte)obj).byteValue();
            index = hashinateLong(value);
        }
        return index;
    }
}
