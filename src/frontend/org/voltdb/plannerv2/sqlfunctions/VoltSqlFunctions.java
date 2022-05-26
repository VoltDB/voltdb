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

package org.voltdb.plannerv2.sqlfunctions;

import com.google.common.collect.ImmutableMultimap;
import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltdb.types.ExpressionType;

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
        // The name of the function
        final private String m_functionName;
        // Whether the function needs its argument to match its declaration exactly.
        // This means that casting (providing a SMALLINT parameter to a BIGINT argument) is not allowed.
        final private boolean m_exactArgumentTypes;
        // Classes of argument types
        final private Class[] m_argumentTypes;
        // Function type
        final private FunctionType m_type;

        protected FunctionDescriptor(String functionName, boolean exactArgumentTypes,
                                     Class[] argumentTypes, FunctionType type) {
            m_functionName = functionName;
            m_exactArgumentTypes = exactArgumentTypes;
            m_argumentTypes = argumentTypes;
            m_type = type;
        }

        public String getFunctionName() {
            return m_functionName;
        }

        public boolean isExactArgumentTypes() {
            return m_exactArgumentTypes;
        }

        public Class[] getArgumentTypes() {
            return m_argumentTypes;
        }

        public FunctionType getType() {
            return m_type;
        }
    }

    public static class ScalarFunctionDescriptor extends FunctionDescriptor {
        // Function ID
        final private int m_functionId;

        public ScalarFunctionDescriptor(String functionName, boolean exactArgumentTypes,
                                        Class[] argumentTypes, int functionId) {
            super(functionName, exactArgumentTypes, argumentTypes, FunctionType.SCALAR);
            m_functionId = functionId;
        }

        public int getFunctionId() {
            return m_functionId;
        }
    }

    public static class AggregateFunctionDescriptor extends FunctionDescriptor {
        // Aggregation type
        final private int m_aggType;

        public AggregateFunctionDescriptor(String functionName, boolean exactArgumentTypes,
                                           Class[] argumentTypes, int aggType) {
            super(functionName, exactArgumentTypes, argumentTypes, FunctionType.AGGREGATE);
            m_aggType = aggType;
        }

        public int getAggType() {
            return m_aggType;
        }
    }

    // The map from method name to SQL FunctionDescriptor
    public static final ImmutableMultimap<Class, FunctionDescriptor> VOLT_SQL_FUNCTIONS =
            ImmutableMultimap.<Class, FunctionDescriptor>builder()
                    // Scalar functions
                    .put(MigrationFunctions.class, new ScalarFunctionDescriptor(
                        "migrating",
                        false,
                        new Class[] {},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_MIGRATING))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                    "bit_shift_left",
                        true,
                        new Class[] {long.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_LEFT))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bit_shift_right",
                        true,
                        new Class[] {long.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_RIGHT))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitAnd",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITAND))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitNot",
                        true,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_BITNOT))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitOr",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITOR))
                    .put(BitwiseFunctions.class, new ScalarFunctionDescriptor(
                        "bitXor",
                        true,
                        new Class[] {long.class, long.class},
                        FunctionCustom.FUNC_BITXOR))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "array_element",
                        false,
                        new Class[] {String.class, int.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_ARRAY_ELEMENT))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "array_length",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_ARRAY_LENGTH))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "field",
                        false,
                        new Class[] {String.class, String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_FIELD))
                    .put(JsonFunctions.class, new ScalarFunctionDescriptor(
                        "set_field",
                        false,
                        new Class[] {String.class, String.class, String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_SET_FIELD))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet6_aton",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET6_ATON))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet6_ntoa",
                        false,
                        new Class[] {byte[].class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET6_NTOA))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet_aton",
                        false,
                        new Class[] {String.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET_ATON))
                    .put(InternetFunctions.class, new ScalarFunctionDescriptor(
                        "inet_ntoa",
                        false,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_INET_ATON))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "hex",
                        false,
                        new Class[] {long.class},
                        FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_HEX))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {String.class, int.class, int.class},
                        FunctionSQL.FUNC_SUBSTRING_CHAR))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {byte[].class, int.class, int.class},
                        FunctionSQL.FUNC_SUBSTRING_BINARY))
                    .put(StringFunctions.class, new ScalarFunctionDescriptor(
                        "substring",
                        false,
                        new Class[] {String.class, int.class},
                        FunctionSQL.FUNC_VOLT_SUBSTRING_CHAR_FROM))
                    // Aggregate functions
                    .put(AggExampleFunctions.class, new AggregateFunctionDescriptor(
                        "aggregate_example",
                        false,
                         new Class[] {double.class},
                         ExpressionType.INVALID.getValue()))
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
    public static class AggExampleFunctions {
        public static double aggregate_example(double values) {
            return 0.0;
        }
    }
}
