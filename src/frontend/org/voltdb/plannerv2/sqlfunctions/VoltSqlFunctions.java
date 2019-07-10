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

package org.voltdb.plannerv2.sqlfunctions;

import com.google.common.collect.ImmutableMultimap;
import org.apache.commons.lang3.tuple.Triple;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltcore.utils.Pair;

/**
 * Implementation of calls to VoltDB SQL functions through Calcite.
 * <p>
 * Steps to implement a function through Calcite:
 * <ul>
 * <li>Under the appropriate static class (create a new one if necessary), create a
 * static method that has the same signature (argument types and return type) as the
 * VoltDB SQL function.
 * <li>Add the method along with its argument classes into {@link #VOLT_SQL_FUNCTIONS}.
 * <li>Use {@link org.apache.calcite.sql.type.JavaToSqlTypeConversionRules} as a guideline
 * for the mapping from Java types to corresponding SQL types.
 *
 * @author Chao Zhou
 * @since 9.1
 */
public class VoltSqlFunctions {
    // A POD holder for representing a SQL function
    public static class FunctionDescriptor {
        // The class that the function is implemented in
        final private Class m_implementor;
        // Whether the function needs its argument to match its declaration exactly.
        // This means that casting (providing a SMALLINT parameter to a BIGINT argument) is not allowed.
        final private boolean m_exactArgumentTypes;
        // Classes of argument types
        final private Class[] m_argumentTypes;

        private FunctionDescriptor(Class implementor, boolean exactArgumentTypes, Class[] argumentTypes) {
            m_implementor = implementor;
            m_exactArgumentTypes = exactArgumentTypes;
            m_argumentTypes = argumentTypes;
        }

        public Class getImplementor() {
            return m_implementor;
        }

        public boolean isExactArgumentTypes() {
            return m_exactArgumentTypes;
        }

        public Class[] getArgumentTypes() {
            return m_argumentTypes;
        }
    }

    // The map from method name to SQL FunctionDescriptor
    public static final ImmutableMultimap<String, FunctionDescriptor> VOLT_SQL_FUNCTIONS =
            ImmutableMultimap.<String, FunctionDescriptor>builder()
                    .put("migrating", new FunctionDescriptor(MigrationFunctions.class, false, new Class[] {}))
                    .put("bit_shift_left", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class, int.class}))
                    .put("bit_shift_right", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class, int.class}))
                    .put("bitAnd", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class, long.class}))
                    .put("bitNot", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class}))
                    .put("bitOr", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class, long.class}))
                    .put("bitXor", new FunctionDescriptor(BitwiseFunctions.class, true, new Class[] {long.class, long.class}))
                    .put("inet6_aton", new FunctionDescriptor(InternetFunctions.class, true, new Class[] {String.class}))
                    .put("inet6_ntoa", new FunctionDescriptor(InternetFunctions.class, true, new Class[] {byte[].class}))
                    .put("inet_aton", new FunctionDescriptor(InternetFunctions.class, true, new Class[] {String.class}))
                    .put("inet_ntoa", new FunctionDescriptor(InternetFunctions.class, true, new Class[] {long.class}))
                    .put("hex", new FunctionDescriptor(StringFunctions.class, false, new Class[] {long.class}))
                    .build();

    //-------------------------------------------------------------
    //                   volt extend sql functions
    //-------------------------------------------------------------

    // We only need the sql function signature in validate&plan phase. The return value doesn't matter.
    // Calcite won't evaluate the function during planning

    // Migration functions
    public static class MigrationFunctions {
        public static boolean migrating() {
            return true;
        }
    }

    // Bitwise functions
    public static class BitwiseFunctions {
        public static long bit_shift_left(long value, int offset) {
            return 0;
        }

        public static long bit_shift_right(long value, int offset) {
            return 0;
        }

        public static long bitAnd(long value1, long value2) {
            return 0;
        }

        public static long bitNot(long value) {
            return 0;
        }

        public static long bitOr(long value1, long value2) {
            return 0;
        }

        public static long bitXor(long value1, long value2) {
            return 0;
        }
    }

    // Internet functions
    public static class InternetFunctions {
        public static byte[] inet6_aton(String address) {
            return null;
        }

        public static String inet6_ntoa(byte[] ip) {
            return null;
        }

        public static long inet_aton(String address) {
            return 0;
        }

        public static String inet_ntoa(long ip) {
            return null;
        }
    }

    // String functions
    public static class StringFunctions {
        public static String hex(long value) {
            return "";
        }
    }
}
