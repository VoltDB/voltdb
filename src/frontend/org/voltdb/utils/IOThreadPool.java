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

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import org.voltcore.utils.CoreUtils;

import java.util.concurrent.TimeUnit;

/**
 * Thread pool for performing blocking disk IO operations
 */
public class IOThreadPool {
    private static final int POOL_SIZE = Integer.getInteger("IO_THREADPOOL_SIZE", 32);
    private static ListeningExecutorService m_stripes[] = new ListeningExecutorService[POOL_SIZE];

    static {
        for (int ii = 0; ii < m_stripes.length; ii++) {
            m_stripes[ii] =
                    CoreUtils.getCachedSingleThreadExecutor("IO pool thread - " + ii, TimeUnit.MINUTES.toMillis(5));
        }
    }

    public static ListeningExecutorService getService(Object key) {
        return m_stripes[smear(key.hashCode()) % m_stripes.length];
    }

    /*
     * This method was written by Doug Lea with assistance from members of JCP
     * JSR-166 Expert Group and released to the public domain, as explained at
     * http://creativecommons.org/licenses/publicdomain
     *
     * As of 2010/06/11, this method is identical to the (package private) hash
     * method in OpenJDK 7's java.util.HashMap class.
     */
    // Copied from java/com/google/common/collect/Hashing.java
    private static int smear(int hashCode) {
        hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
        return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
    }
}

