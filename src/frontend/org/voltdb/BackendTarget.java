/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

/**
 * Specifies whether the system should be run on the native C++
 * backend for VoltDB, or if the system should use a JDBC
 * wrapper around HSQLDB.
 *
 * HSQLDB is pure java, making the system very portable, and it
 * supports a wide range of SQL. On the other hand, it's not as
 * fast and only supports a single partition. It's best used
 * for testing.
 *
 */
public enum BackendTarget {
    NATIVE_EE_JNI("jni"),
    NATIVE_EE_IPC("ipc"),
    NATIVE_EE_VALGRIND_IPC("valgrind_ipc"),
    HSQLDB_BACKEND("hsqldb"),
    NONE("none");
    private BackendTarget(String display) { this.display = display; }
    public final String display;
}
