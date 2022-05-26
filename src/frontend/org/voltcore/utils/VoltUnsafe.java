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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import sun.misc.Unsafe;

/*
 * <p>Wrapper for {@link sun.misc.Unsafe} class.</p>
 */
public abstract class VoltUnsafe {

    public static final boolean IS_JAVA8 = System.getProperty("java.version").startsWith("1.8");

    /** Unsafe. */
    private static final Unsafe UNSAFE = unsafe();

    /** Cleaner code for direct {@code java.nio.ByteBuffer}. */
    public static final DirectBufferCleaner DIRECT_BYTE_BUFFER_CLEANER = IS_JAVA8
                    ? new ReflectiveDirectBufferCleaner()
                    : new UnsafeDirectBufferCleaner();

    /**
     * Invokes some method on {@code sun.misc.Unsafe} instance.
     *
     * @param mtd Method.
     * @param args Arguments.
     */
    public static Object invoke(Method mtd, Object... args) {
        try {
            return mtd.invoke(UNSAFE, args);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unsafe invocation failed [cls=" + UNSAFE.getClass() + ", mtd=" + mtd + ']', e);
        }
    }

    /**
     * Cleans direct {@code java.nio.ByteBuffer}
     *
     * @param buf Direct buffer.
     */
    public static void cleanDirectBuffer(ByteBuffer buf) {
        assert buf.isDirect();
        DIRECT_BYTE_BUFFER_CLEANER.clean(buf);
    }


    /**
     * @return Instance of Unsafe class.
     */
    private static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged
                        (new PrivilegedExceptionAction<Unsafe>() {
                            @Override public Unsafe run() throws Exception {
                                Field f = Unsafe.class.getDeclaredField("theUnsafe");

                                f.setAccessible(true);

                                return (Unsafe)f.get(null);
                            }
                        });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }
}
