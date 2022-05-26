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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Vector;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltcore.utils.ByteBufferInputStream;
import org.voltdb.catalog.Function;
import org.voltdb.compiler.statements.CreateAggregateFunctionFromClass;
import org.voltdb.utils.SerializationHelper;

/**
 * This class maintains the necessary information for each UDAF including the class instance
 * and the method ID for the UDAF implementation. We run UDAFs from this runner.
 * @author russelhu
 *
 */
public class UserDefinedAggregateFunctionRunner extends UserDefinedFunctionRunner {
    final String m_functionName;
    final int m_functionId;
    final String m_className;
    Method m_startMethod;
    Method m_assembleMethod;
    Method m_combineMethod;
    Method m_endMethod;
    Class<?> m_funcClass;
    Method[] m_functionMethods;
    Vector<Object> m_functionInstances;
    final VoltType[] m_paramTypes;
    final boolean[] m_boxUpByteArray;
    final VoltType m_returnType;
    final int m_paramCount;

    public static Object readObject(ByteBuffer buffer) throws IOException, ClassNotFoundException {
        // Sanity check the size against the remaining buffer size.
        if (VAR_LEN_SIZE > buffer.remaining()) {
            throw new RuntimeException(String.format(
                    "Can't read varbinary size as %d byte integer " + "from buffer with %d bytes remaining.",
                    VAR_LEN_SIZE, buffer.remaining()));
        }
        final int len = buffer.getInt();
        if (len == VoltTable.NULL_STRING_INDICATOR) {
            return null;
        }
        if (len < 0) {
            throw new RuntimeException("Invalid object length.");
        }
        int originalLimit = buffer.limit();
        buffer.limit(buffer.position() + len);
        ByteBuffer slice = buffer.slice();
        buffer.position(buffer.limit());
        buffer.limit(originalLimit);

        try (ByteBufferInputStream bis = new ByteBufferInputStream(slice);
                ObjectInput in = new UDAFObjectInputStream(bis)) {
            return in.readObject();
        }
    }

    public UserDefinedAggregateFunctionRunner(Function catalogFunction, Class<?> funcClass) {
        this(catalogFunction.getFunctionname(), catalogFunction.getFunctionid(),
                catalogFunction.getClassname(), funcClass);
    }

    public UserDefinedAggregateFunctionRunner(String functionName, int functionId, String className, Class<?> funcClass) {

        m_functionName = functionName;
        m_functionId = functionId;
        m_className = className;
        m_funcClass = funcClass;
        m_functionMethods = funcClass.getDeclaredMethods();
        m_functionInstances = new Vector<Object>();

        // Call the static method in class "CreateAggregateFunctionFromClass"
        Map<String, Method> methods = CreateAggregateFunctionFromClass.retrieveMethodsFromClass(funcClass);
        m_startMethod = methods.get("start");
        m_assembleMethod = methods.get("assemble");
        m_combineMethod = methods.get("combine");
        m_endMethod = methods.get("end");

        Class<?>[] paramTypeClasses = m_assembleMethod.getParameterTypes();
        m_paramCount = paramTypeClasses.length;
        m_paramTypes = new VoltType[m_paramCount];
        m_boxUpByteArray = new boolean[m_paramCount];
        for (int i = 0; i < m_paramCount; i++) {
            m_paramTypes[i] = VoltType.typeFromClass(paramTypeClasses[i]);
            m_boxUpByteArray[i] = paramTypeClasses[i] == Byte[].class;
        }
        m_returnType = VoltType.typeFromClass(m_endMethod.getReturnType());

        m_logger.debug(String.format("The user-defined function manager is defining aggregate function %s (ID = %s)",
                m_functionName, m_functionId));

        // We register the token again when initializing the user-defined function manager because
        // in a cluster setting the token may only be registered on the node where the CREATE FUNCTION DDL
        // is executed. We uses a static map in FunctionDescriptor to maintain the token list.
        FunctionForVoltDB.registerTokenForUDF(m_functionName, m_functionId, m_returnType, m_paramTypes, true);
    }

    private void addFunctionInstance() {
        try {
            Object tempFunctionInstance = m_funcClass.newInstance();
            m_functionInstances.add(tempFunctionInstance);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error instantiating function \"%s\"", m_className), e);
        }
    }

    public void start() throws Throwable {
        addFunctionInstance();
        m_startMethod.invoke(m_functionInstances.lastElement());
    }

    public void assemble(ByteBuffer udfBuffer, int udafIndex) throws Throwable {
        // assemble method has only one argument, which is defined in the voltUDAggregate interface
        // Note: changes needed when we need to support UDAF that takes more than one columns.
        assert(m_paramCount == 1);
        // retrieve the argument count from udfbuffer
        int argNum = udfBuffer.getInt();
        while (argNum-- > 0) {
            // read the buffer multiple times for each argument and pass it to assmble method
            Object[] paramsIn = new Object[m_paramCount];
            paramsIn[0] = getValueFromBuffer(udfBuffer, m_paramTypes[0]);
            if (m_boxUpByteArray[0]) {
                paramsIn[0] = SerializationHelper.boxUpByteArray((byte[])paramsIn[0]);
            }
            m_assembleMethod.invoke(m_functionInstances.get(udafIndex), paramsIn);
        }
    }

    public void combine(Object other, int udafIndex) throws Throwable {
        m_combineMethod.invoke(m_functionInstances.get(udafIndex), other);
    }

    public Object end(int udafIndex) throws Throwable {
        Object result = m_endMethod.invoke(m_functionInstances.get(udafIndex));
        if (udafIndex == m_functionInstances.size() - 1) {
            m_functionInstances.clear();
        }
        return result;
    }

    public VoltType getReturnType() {
        return m_returnType;
    }

    public Object getFunctionInstance(int udafIndex) {
        return m_functionInstances.get(udafIndex);
    }

    public void clearFunctionInstance(int udafIndex) {
        if (udafIndex == m_functionInstances.size() - 1) {
            m_functionInstances.clear();
        }
    }

    static class UDAFObjectInputStream extends ObjectInputStream {
        public UDAFObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String name = desc.getName();
            try {
                return Class.forName(name, true,
                        VoltDB.instance().getCatalogContext().m_catalogInfo.m_jarfile.getLoader());
            } catch (ClassNotFoundException ex) {
                throw ex;
            }
        }
    }
}
