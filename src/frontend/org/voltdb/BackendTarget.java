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

package org.voltdb;

/**
 * Specifies whether the system should be run on the native C++
 * backend for VoltDB, or if the system should use a JDBC
 * wrapper around HSQLDB or PostgreSQL.
 *
 * HSQLDB is pure java, making the system very portable, and it
 * supports a wide range of SQL. On the other hand, it's not as
 * fast and only supports a single partition. It's best used
 * for testing.
 *
 * The PostgreSQL option simply calls a PostgreSQL database via JDBC;
 * the PostGIS option is similar, but uses the PostGIS extension to PostgreSQL.
 * Again, this is used only for testing, specifically, in the SQLCoverage
 * tests, where VoltDB results are compared with PostgreSQL results.
 */
public enum BackendTarget {
                   /*      display,        isIPC, isVGABLE, isVG,  isLTT, isDefJNI */
    NATIVE_EE_JNI(         "jni",          false, true,     false, false, true),
    NATIVE_EE_LARGE_JNI(   "jni_large",    false, true,     false, true,  true),
    NATIVE_EE_SPY_JNI(     "jni_spy",      false, false,    false, false, false),
    NATIVE_EE_IPC(         "ipc",          true,  false,    false, false, false),
    NATIVE_EE_VALGRIND_IPC("valgrind_ipc", true,  false,    true,  false, false),
    HSQLDB_BACKEND(        "hsqldb",       false, false,    false, false, false),
    POSTGRESQL_BACKEND(     "postgresql",  false, false,    false, false, false),
    POSTGIS_BACKEND(        "postgis",     false, false,    false, false, false),
    NONE(                   "none",        false, false,    false, false, false);

    private BackendTarget(String display,
                          boolean isIPC,
                          boolean isValgrindable,
                          boolean isValgrindTarget,
                          boolean isLargeTempTableTarget,
                          boolean isDefaulJNITarget) {
        this.display = display;
        this.isIPC = isIPC;
        this.isValgrindable = isValgrindable;
        this.isValgrindTarget = isValgrindTarget;
        this.isLargeTempTableTarget = isLargeTempTableTarget;
        this.isDefaultJNITarget = isDefaulJNITarget;
    }
    public final String display;
    public final boolean isIPC;
    // True iff this target can be used with Valgrind.
    // The target need not be a valgrind target, but it's
    // sensible to convert it to be a valgrind target.
    public final boolean isValgrindable;
    // True if this target is actually a valgrind
    // target.  That is to say, we are going to use
    // valgrind with this target.
    public final boolean isValgrindTarget;
    // True if this target is a large temp table target.
    public final boolean isLargeTempTableTarget;
    // True if this is a JNI target with no special engine properties.
    public final boolean isDefaultJNITarget;
}
