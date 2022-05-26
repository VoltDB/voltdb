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

package org.voltcore.utils;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

/**
 * {@link DirectBufferCleaner} implementation based on {@code Unsafe.invokeCleaner} method.
 *
 * Note: This implementation will work only for Java 9+.
 */
public class UnsafeDirectBufferCleaner implements DirectBufferCleaner {
    /** Cleaner method. */
    private final Method invokeCleanerMtd;

    /** */
    public UnsafeDirectBufferCleaner() {
        try {
            invokeCleanerMtd = Unsafe.class.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Reflection failure: no sun.misc.Unsafe.invokeCleaner() method found", e);
        }
    }

    @Override
    public void clean(ByteBuffer buf) {
        VoltUnsafe.invoke(invokeCleanerMtd, buf);
    }
}
