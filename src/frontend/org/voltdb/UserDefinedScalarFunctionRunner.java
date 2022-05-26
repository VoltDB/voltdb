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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Function;
import org.voltdb.utils.SerializationHelper;

/**
 * This class maintains the necessary information for each UDF including the class instance and
 * the method ID for the UDF implementation. We run UDFs from this runner.
 */
public class UserDefinedScalarFunctionRunner extends UserDefinedFunctionRunner {

    final String m_functionName;
    final int m_functionId;
    final Object m_functionInstance;
    Method m_functionMethod;
    final VoltType[] m_paramTypes;
    final boolean[] m_boxUpByteArray;
    final VoltType m_returnType;
    final int m_paramCount;

    public UserDefinedScalarFunctionRunner(Function catalogFunction, Object funcInstance) {
        this(catalogFunction.getFunctionname(), catalogFunction.getFunctionid(),
                catalogFunction.getMethodname(), funcInstance);
    }

    public UserDefinedScalarFunctionRunner(String functionName, int functionId, String methodName, Object funcInstance) {
        m_functionName = functionName;
        m_functionId = functionId;
        m_functionInstance = funcInstance;
        m_functionMethod = null;

        initFunctionMethod(methodName);
        Class<?>[] paramTypeClasses = m_functionMethod.getParameterTypes();
        m_paramCount = paramTypeClasses.length;
        m_paramTypes = new VoltType[m_paramCount];
        m_boxUpByteArray = new boolean[m_paramCount];
        for (int i = 0; i < m_paramCount; i++) {
            m_paramTypes[i] = VoltType.typeFromClass(paramTypeClasses[i]);
            m_boxUpByteArray[i] = paramTypeClasses[i] == Byte[].class;
        }
        m_returnType = VoltType.typeFromClass(m_functionMethod.getReturnType());

        m_logger.debug(String.format("The user-defined function manager is defining function %s (ID = %s)",
                m_functionName, m_functionId));

        // We register the token again when initializing the user-defined function manager because
        // in a cluster setting the token may only be registered on the node where the CREATE FUNCTION DDL
        // is executed. We uses a static map in FunctionDescriptor to maintain the token list.
        FunctionForVoltDB.registerTokenForUDF(m_functionName, m_functionId, m_returnType, m_paramTypes, false);
    }

    private void initFunctionMethod(String methodName) {
        for (final Method m : m_functionInstance.getClass().getDeclaredMethods()) {
            if (m.getName().equals(methodName)) {
                if (! Modifier.isPublic(m.getModifiers())) {
                    continue;
                }
                if (Modifier.isStatic(m.getModifiers())) {
                    continue;
                }
                if (m.getReturnType().equals(Void.TYPE)) {
                    continue;
                }
                m_functionMethod = m;
                break;
            }

        }
        if (m_functionMethod == null) {
            throw new RuntimeException(
                    String.format("Error loading function %s: cannot find the %s() method.",
                            m_functionName, methodName));
        }
    }

    public Object call(ByteBuffer udfBuffer) throws Throwable {
        Object[] paramsIn = new Object[m_paramCount];
        for (int i = 0; i < m_paramCount; i++) {
            paramsIn[i] = getValueFromBuffer(udfBuffer, m_paramTypes[i]);
            if (m_boxUpByteArray[i]) {
                paramsIn[i] = SerializationHelper.boxUpByteArray((byte[])paramsIn[i]);
            }
        }
        return m_functionMethod.invoke(m_functionInstance, paramsIn);
    }

    public VoltType getReturnType() {
        return m_returnType;
    }

    public String getFunctionName() {
        return m_functionName;
    }

}
