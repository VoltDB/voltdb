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
import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltdb.catalog.Function;

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
    // A list of function types
    public enum FunctionType{
        // Scalar function
        SCALAR,
        // Aggregate function
        AGGREGATE
    }
    // A POD holder for representing a SQL function
    public static class FunctionDescriptor {
        // Whether the function needs its argument to match its declaration exactly.
        // This means that casting (providing a SMALLINT parameter to a BIGINT argument) is not allowed.
        final private boolean m_exactArgumentTypes;
        // Classes of argument types
        final private Class[] m_argumentTypes;
        // Function ID
        final private int m_functionId;
        // Function type
        final private FunctionType m_type;


        protected FunctionDescriptor(boolean exactArgumentTypes,
                                     Class[] argumentTypes, int functionId, FunctionType type) {
            m_exactArgumentTypes = exactArgumentTypes;
            m_argumentTypes = argumentTypes;
            m_functionId = functionId;
            m_type = type;
        }

        public boolean isExactArgumentTypes() {
            return m_exactArgumentTypes;
        }

        public Class[] getArgumentTypes() {
            return m_argumentTypes;
        }

        public int getFunctionId() {
            return m_functionId;
        }

        public FunctionType getType() {
            return m_type;
        }
    }

    public static class ScalarFunctionDescriptor extends FunctionDescriptor {
        // The name of the method that implements the scalar function
        final private String m_implementor;

        public ScalarFunctionDescriptor(String implementor, boolean exactArgumentTypes,
                                        Class[] argumentTypes, int functionId, FunctionType type) {
            super(exactArgumentTypes, argumentTypes, functionId, FunctionType.SCALAR);
            m_implementor = implementor;
        }

        public String getImplementor() {
            return m_implementor;
        }
    }

    public static class AggregateFunctionDescriptor extends FunctionDescriptor {
        // The name of the methods that implement the aggregate function
        final private String m_startImplementor;
        final private String m_assembleImplementor;
        final private String m_combineImplementor;
        final private String m_endImplementor;

        public AggregateFunctionDescriptor(String startImplementor, String assembleImplementor,
                                           String combineImplementor, String endImplementor,
                                           boolean exactArgumentTypes, Class[] argumentTypes,
                                           int functionId, FunctionType type) {
            super(exactArgumentTypes, argumentTypes, functionId, FunctionType.AGGREGATE);
            m_startImplementor = startImplementor;
            m_assembleImplementor = assembleImplementor;
            m_combineImplementor = combineImplementor;
            m_endImplementor = endImplementor;
        }

        public String getStartImplementor() {
            return m_startImplementor;
        }

        public String getAddImplementor() {
            return m_assembleImplementor;
        }

        public String getMergeImplementor() {
            return m_combineImplementor;
        }

        public String getEndImplementor() {
            return m_endImplementor;
        }
    }

    // The map from method name to SQL FunctionDescriptor
    public static final ImmutableMultimap<Class, FunctionDescriptor> VOLT_SQL_FUNCTIONS =
            ImmutableMultimap.<Class, FunctionDescriptor>builder()
                    .put(MigrationFunctions.class, new ScalarFunctionDescriptor(
                        "migrating",
                        false,
                        new Class[] {},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_MIGRATING,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                    "bit_shift_left",
                        true,
                        new Class[] {long.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_LEFT,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bit_shift_right",
                        true,
                        new Class[] {long.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_RIGHT,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitAnd",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITAND,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitNot",
                        true,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BITNOT,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitOr",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITOR,
                        FunctionType.SCALAR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitXor",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITXOR,
                        FunctionType.SCALAR))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "array_element",
                        false,
                        new Class[] {String.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_ARRAY_ELEMENT,
                        FunctionType.SCALAR))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "array_length",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_ARRAY_LENGTH,
                        FunctionType.SCALAR))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "field",
                        false,
                        new Class[] {String.class, String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_FIELD,
                        FunctionType.SCALAR))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "set_field",
                        false,
                        new Class[] {String.class, String.class, String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_SET_FIELD,
                        FunctionType.SCALAR))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet6_aton",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET6_ATON,
                        FunctionType.SCALAR))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet6_ntoa",
                        false,
                        new Class[] {byte[].class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET6_NTOA,
                        FunctionType.SCALAR))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet_aton",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET_ATON,
                        FunctionType.SCALAR))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet_ntoa",
                        false,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET_ATON,
                        FunctionType.SCALAR))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "hex",
                        false,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_HEX,
                        FunctionType.SCALAR))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {String.class, int.class, int.class},
                        FunctionSQL.FUNC_SUBSTRING_CHAR,
                        FunctionType.SCALAR))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {byte[].class, int.class, int.class},
                        FunctionSQL.FUNC_SUBSTRING_BINARY,
                        FunctionType.SCALAR))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {String.class, int.class},
                        FunctionSQL.FUNC_VOLT_SUBSTRING_CHAR_FROM,
                        FunctionType.SCALAR))
                    .build();

    //-------------------------------------------------------------
    //                   volt extend sql scalar functions
    //-------------------------------------------------------------

    // We only need the sql function signature in validate&plan phase. The return value doesn't matter.
    // Calcite won't evaluate the function during planning

    //
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

    // JSON functions
    public static class JsonFunctions {
        public static String array_element(String array, int position) {
            return "";
        }

        public static int array_length(String array) {
            return 0;
        }

        public static String field(String jsonString, String fieldName) {
            return "";
        }

        public static String set_field(String colName, String fieldName, String fieldValue) {
            return "";
        }
    }

    // String functions
    public static class StringFunctions {
        public static String hex(long value) {
            return "";
        }

        public static String substring(String s, int position, int length) {
            return "";
        }

        public static String substring(byte[] b, int position, int length) {
            return "";
        }

        public static String substring(String s, int position) {
            return "";
        }
    }

    //-------------------------------------------------------------
    //                   volt extend sql aggregate functions
    //-------------------------------------------------------------
    public static class StdDev {

    }
}
