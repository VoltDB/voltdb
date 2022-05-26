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

package org.voltdb.compiler.statements;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.collect.ImmutableSet;

/**
 * This is the base class for CreateFunctionFromMethod and CreateAggregateFunctionFromClass
 */
public abstract class CreateFunction extends StatementProcessor {

    public CreateFunction(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    protected static VoltLogger m_logger = new VoltLogger("UDF");

    static int ID_NOT_DEFINED = -1;
    static final ImmutableSet<Class<?>> m_allowedDataTypes = ImmutableSet.of(
            byte.class,
            byte[].class,
            short.class,
            int.class,
            long.class,
            double.class,
            Byte.class,
            Byte[].class,
            Short.class,
            Integer.class,
            Long.class,
            Double.class,
            BigDecimal.class,
            String.class,
            TimestampType.class,
            GeographyPointValue.class,
            GeographyValue.class
            );

    static final ImmutableSet<String> m_builtInAggregateFunctions = ImmutableSet.of(
            "SUM", "COUNT", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT");

     /**
     * Find out if the function is defined.  It might be defined in the
     * FunctionForVoltDB table.  It also might be in the VoltXML.
     *
     * @param functionName
     * @return
     */
    protected boolean isDefinedFunctionName(String functionName) {
        return FunctionForVoltDB.isFunctionNameDefined(functionName)
                || FunctionSQL.isFunction(functionName)
                || FunctionCustom.getFunctionId(functionName) != ID_NOT_DEFINED
                || (null != m_schema.findChild("ud_function", functionName)
                || isBuiltInAggregateFunction(functionName));
    }

    protected boolean isBuiltInAggregateFunction(String functionName) {
        return m_builtInAggregateFunctions.contains(functionName.toUpperCase());
    }

    public static boolean isAllowedDataType(Class<?> type) {
        return m_allowedDataTypes.contains(type);
    }

}
