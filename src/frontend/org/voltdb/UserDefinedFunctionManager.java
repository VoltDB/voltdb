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

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Function;
import org.voltdb.utils.JavaBuiltInFunctions;
import org.voltdb.UserDefinedAggregateFunctionRunner;
import org.voltdb.UserDefinedScalarFunctionRunner;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * This is the Java class that manages the UDF class instances, and also the invocation logics.
 */
public class UserDefinedFunctionManager {

    static final String ORGVOLTDB_FUNCCNAME_ERROR_FMT =
            "VoltDB does not support function classes with package names " +
            "that are prefixed with \"org.voltdb\". Please use a different " +
            "package name and retry. The class name was %s.";
    static final String UNABLETOLOAD_ERROR_FMT =
            "VoltDB was unable to load a function (%s) which was expected to be " +
            "in the catalog jarfile and will now exit.";

    ImmutableMap<Integer, UserDefinedScalarFunctionRunner> m_udfs = ImmutableMap.of();
    ImmutableMap<Integer, UserDefinedAggregateFunctionRunner> m_udafs = ImmutableMap.of();

    public UserDefinedScalarFunctionRunner getFunctionRunnerById(int functionId) {
        return m_udfs.get(functionId);
    }

    public UserDefinedAggregateFunctionRunner getAggregateFunctionRunnerById(int functionId) {
        return m_udafs.get(functionId);
    }

    // Load all the UDFs recorded in the catalog. Instantiate and register them in the system.
    // WARNING: This is called from all sites in parallel but updates a shared static
    //          data structure in FunctionForVoltDB
    public synchronized void loadFunctions(CatalogContext catalogContext) {
        final CatalogMap<Function> catalogFunctions = catalogContext.database.getFunctions();
        // Remove obsolete tokens (scalar)
        for (UserDefinedScalarFunctionRunner runner : m_udfs.values()) {
            // The function that the current UserDefinedScalarFunctionRunner is referring to
            // does not exist in the catalog anymore, we need to remove its token.
            if (catalogFunctions.get(runner.m_functionName) == null) {
                FunctionForVoltDB.deregisterUserDefinedFunction(runner.m_functionName);
            }
        }
        // Remove obsolete tokens (aggregate)
        for (UserDefinedAggregateFunctionRunner runner : m_udafs.values()) {
            // The function that the current UserDefinedAggregateFunctionRunner is referring to
            // does not exist in the catalog anymore, we need to remove its token.
            if (catalogFunctions.get(runner.m_functionName) == null) {
                FunctionForVoltDB.deregisterUserDefinedFunction(runner.m_functionName);
            }
        }
        // Build new UDF runners
        ImmutableMap.Builder<Integer, UserDefinedScalarFunctionRunner> builder =
                            ImmutableMap.<Integer, UserDefinedScalarFunctionRunner>builder();
        ImmutableMap.Builder<Integer, UserDefinedAggregateFunctionRunner> builderAgg =
                            ImmutableMap.<Integer, UserDefinedAggregateFunctionRunner>builder();
        for (final Function catalogFunction : catalogFunctions) {
            final String className = catalogFunction.getClassname();
            Class<?> funcClass = null;
            try {
                funcClass = catalogContext.classForProcedureOrUDF(className);
            }
            catch (final ClassNotFoundException e) {
                if (className.startsWith("org.voltdb.")) {
                    String msg = String.format(ORGVOLTDB_FUNCCNAME_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
                else {
                    String msg = String.format(UNABLETOLOAD_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }
            Object funcInstance = null;
            try {
                funcInstance = funcClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(String.format("Error instantiating function \"%s\"", className), e);
            }
            assert(funcInstance != null);
            if (catalogFunction.getMethodname() == null) {
                // no_method_here -> aggregate function
                builderAgg.put(catalogFunction.getFunctionid(), new UserDefinedAggregateFunctionRunner(catalogFunction, funcClass));
            } else {
                // There is a methodName -> scalar function
                builder.put(catalogFunction.getFunctionid(), new UserDefinedScalarFunctionRunner(catalogFunction, funcInstance));
            }
        }

        loadBuiltInJavaFunctions(builder);
        m_udfs = builder.build();
        m_udafs = builderAgg.build();
    }

    private void loadBuiltInJavaFunctions(ImmutableMap.Builder<Integer, UserDefinedScalarFunctionRunner> builder) {
        // define the function objects
        String[] functionNames = {"format_timestamp"};
        for (String functionName : functionNames) {
            int functionID = FunctionForVoltDB.getFunctionID(functionName);
            builder.put(functionID, new UserDefinedScalarFunctionRunner(functionName,
                    functionID, functionName, new JavaBuiltInFunctions()));
        }
    }
}
