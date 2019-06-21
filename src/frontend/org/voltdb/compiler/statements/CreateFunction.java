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

/**
 * This is the base class for CreateFunctionFromMethod and CreateAggregateFunctionFromClass
 */
public abstract class CreateFunction extends StatementProcessor {

    public CreateFunction(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    protected static VoltLogger m_logger = new VoltLogger("UDF");

    static int ID_NOT_DEFINED = -1;
    static Set<Class<?>> m_allowedDataTypes = new HashSet<>();

    static {
        m_allowedDataTypes.add(byte.class);
        m_allowedDataTypes.add(byte[].class);
        m_allowedDataTypes.add(short.class);
        m_allowedDataTypes.add(int.class);
        m_allowedDataTypes.add(long.class);
        m_allowedDataTypes.add(double.class);
        m_allowedDataTypes.add(Byte.class);
        m_allowedDataTypes.add(Byte[].class);
        m_allowedDataTypes.add(Short.class);
        m_allowedDataTypes.add(Integer.class);
        m_allowedDataTypes.add(Long.class);
        m_allowedDataTypes.add(Double.class);
        m_allowedDataTypes.add(BigDecimal.class);
        m_allowedDataTypes.add(String.class);
        m_allowedDataTypes.add(TimestampType.class);
        m_allowedDataTypes.add(GeographyPointValue.class);
        m_allowedDataTypes.add(GeographyValue.class);
    }

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
                || (null != m_schema.findChild("ud_function", functionName));
    }

}