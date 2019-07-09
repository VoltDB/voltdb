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
import org.voltcore.utils.Pair;

/**
 * Implementation of calls to VoltDB SQL functions through Calcite.
 * <p>
 * Steps to implement a function through Calcite:
 * <ul>
 * <li>Under the appropriate static class, create a static method that has the same
 * signature as the VoltDB SQL function.
 * <li>Add the method along with its argument classes into {@link #VOLT_SQL_FUNCTIONS}.
 * <li>Use {@link org.apache.calcite.sql.type.JavaToSqlTypeConversionRules} as a guideline
 * for the mapping from Java types to corresponding SQL types.
 *
 * @author Chao Zhou
 * @since 9.1
 */
public class VoltSqlFunctions {
    // The map from method name to an ImmutableList of classes for registering
    // volt extend sql functions. The first class in the list implements the method,
    // the classes that follow are argument types.
    public static final ImmutableMultimap<String, Triple<Class, Boolean, Class []>> VOLT_SQL_FUNCTIONS =
            ImmutableMultimap.<String, Triple<Class, Boolean, Class []>>builder()
                    .put("migrating", Triple.of(MigrationFunctions.class, false, new Class []{}))
                    .put("bitShiftLeft", Triple.of(BitwiseFunctions.class, true, new Class []{long.class, int.class}))
                    .put("bitShiftRight", Triple.of(BitwiseFunctions.class, true, new Class []{long.class, int.class}))
                    .put("bitAnd", Triple.of(BitwiseFunctions.class, true, new Class []{long.class, long.class}))
                    .put("bitNot", Triple.of(BitwiseFunctions.class, true, new Class []{long.class}))
                    .put("bitOr", Triple.of(BitwiseFunctions.class, true, new Class []{long.class, long.class}))
                    .put("bitXor", Triple.of(BitwiseFunctions.class, true, new Class []{long.class, long.class}))
                    .put("hex", Triple.of(StringFunctions.class, false, new Class []{long.class}))
                    .build();

    //-------------------------------------------------------------
    //                   volt extend sql functions
    //-------------------------------------------------------------

    // We only need the sql function signature in validate&plan phase. The return value doesn't matter.
    // Calcite won't evaluate the function during planning

    // MIGRATING() function
    public static class MigrationFunctions {
        public static boolean migrating() {
            return true;
        }
    }

    // Bitwise functions
    public static class BitwiseFunctions {
        public static long bitShiftLeft(long value, int offset) {
            return 0;
        }

        public static long bitShiftRight(long value, int offset) {
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

    // String functions
    public static class StringFunctions {
        public static String hex(long value) {
            return "";
        }
    }
}
