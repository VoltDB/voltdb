/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import sun.misc.Unsafe;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * {@link DirectBufferCleaner} implementation based on {@code Unsafe.invokeCleaner} method.
 *
 * Note: This implementation will work only for Java 9+.
 */
public class UnsafeDirectBufferCleaner implements DirectBufferCleaner {
    /** Cleaner method. */
    private final Method invokeCleanerMtd;
    private final Method cleanerMtd;

    /** */
    public UnsafeDirectBufferCleaner() {
        try {
            cleanerMtd = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No sun.nio.ch.DirectBuffer.cleaner() method found", e);
        }

        try {
            invokeCleanerMtd = Unsafe.class.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Reflection failure: no sun.misc.Unsafe.invokeCleaner() method found", e);
        }
    }

    @Override
    public boolean clean(ByteBuffer buf) {
        try {
            Object cleaner = cleanerMtd.invoke(buf);
            if (cleaner == null) {
                return false;
            }
            VoltUnsafe.invoke(invokeCleanerMtd, buf);
            return true;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to invoke direct buffer cleaner", e);
        }
    }
}