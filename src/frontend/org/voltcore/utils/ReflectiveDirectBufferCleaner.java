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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * {@link DirectBufferCleaner} implementation based on {@code sun.misc.Cleaner} and
 * {@code sun.nio.ch.DirectBuffer.cleaner()} method.
 *
 * Note: This implementation will not work on Java 9+.
 */
public class ReflectiveDirectBufferCleaner implements DirectBufferCleaner {
    /** Cleaner method. */
    private final Method cleanerMtd;

    /** Clean method. */
    private final Method cleanMtd;

    /** */
    public ReflectiveDirectBufferCleaner() {
        try {
            cleanerMtd = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No sun.nio.ch.DirectBuffer.cleaner() method found", e);
        }

        try {
            cleanMtd = Class.forName("sun.misc.Cleaner").getMethod("clean");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No sun.misc.Cleaner.clean() method found", e);
        }
    }

    @Override
    public void clean(ByteBuffer buf) {
        try {
            Object cleaner = cleanerMtd.invoke(buf);
            assert cleaner != null;
            cleanMtd.invoke(cleaner);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to invoke direct buffer cleaner", e);
        }
    }
}
