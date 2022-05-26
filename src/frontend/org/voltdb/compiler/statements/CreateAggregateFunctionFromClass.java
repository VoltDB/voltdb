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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.HashMap;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.VoltUDAggregate;

/**
 * Process CREATE AGGREGATE FUNCTION <function-name> FROM CLASS <class-name>
 */
public class CreateAggregateFunctionFromClass extends CreateFunction {

    public CreateAggregateFunctionFromClass(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    // Check the validity of the function name or loaded class
    // If it is valid, return the function class
    // Otherwise, throw an exception
    private Class<?> isSQLParamValid(String functionName, String className, String shortName)
        throws VoltCompilerException, RuntimeException, Error {
        // Check if the function is already defined
        if (isDefinedFunctionName(functionName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Function \"%s\" is already defined.",
                    functionName));
        }

        // Load the function class
        Class<?> funcClass;
        try {
            funcClass = Class.forName(className, true, m_classLoader);
        } catch (Throwable cause) {
            // We are here because either the class was not found or the class was found and
            // the initializer of the class threw an error we can't anticipate. So we will
            // wrap the error with a runtime exception that we can trap in our code.
            if (CoreUtils.isStoredProcThrowableFatalToServer(cause)) {
                throw (Error)cause;
            } else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Cannot load class for user-defined aggregate function: %s",
                        className), cause);
            }
        }

        // Check whether this class can be instantiated or not
        try {
            funcClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error instantiating class \"%s\"", shortName), e);
        }

        // The UDAF must implement interfaces "Serializable" and "VoltUDAggregate"
        if (!Serializable.class.isAssignableFrom(funcClass) || !VoltUDAggregate.class.isAssignableFrom(funcClass)) {
            throw m_compiler.new VoltCompilerException(
                    "Cannot define a aggregate function without implementing Serializable or " +
                            "VoltUDAggregate in the class declaration");
        }

        // Loaded class can not be abstract
        if (Modifier.isAbstract(funcClass.getModifiers())) {
            throw m_compiler.new VoltCompilerException(String.format(
                "Cannot define a aggregate function using an abstract class %s",
                className));
        }

        return funcClass;
    }

    public static Map<String, Method> retrieveMethodsFromClass(Class<?> funcClass)
    throws RuntimeException {
        Map<String, Method> methods = new HashMap<>();
        for (Method m : funcClass.getDeclaredMethods()) {
            // method must public and only be public
            if (m.getModifiers() != Modifier.PUBLIC) {
                continue;
            }
            String funcName = m.getName();
            switch(funcName) {
                case "start":
                    if (m.getReturnType().equals(Void.TYPE) && m.getParameterCount() == 0) {
                        methods.put("start", m);
                    }
                    break;
                case "assemble":
                    if (m.getReturnType().equals(Void.TYPE) && m.getParameterCount() == 1) {
                        Class<?> paramTypeClass = m.getParameterTypes()[0];
                        if (isAllowedDataType(paramTypeClass)) {
                            methods.put("assemble", m);
                        } else {
                            throw new RuntimeException(String.format("Unsupported parameter value type: %s", paramTypeClass.getName()));
                        }
                    }
                    break;
                case "combine":
                    if (m.getReturnType().equals(Void.TYPE) && m.getParameterCount() == 1) {
                        Class<?> paramTypeClass = m.getParameterTypes()[0];
                        if (funcClass.equals(paramTypeClass)) {
                            methods.put("combine", m);
                        } else {
                            throw new RuntimeException(String.format("Parameter type must be instance of Class: %s", funcClass.getName()));
                        }
                    }
                    break;
                case "end":
                    if (m.getParameterCount() == 0) {
                        Class<?> returnTypeClass = m.getReturnType();
                        if (!returnTypeClass.equals(Void.TYPE) && isAllowedDataType(returnTypeClass)) {
                            methods.put("end", m);
                        } else {
                            throw new RuntimeException(String.format("Unsupported return value type: %s", returnTypeClass.getName()));
                        }
                    }
                    break;
                default:
                    // do nothing here
            }
        }
        return methods;
    }

    private VoltXMLElement createVoltXMLElementForUDAF(String functionName, String className, Map<String, Method> methods)
        throws RuntimeException {
        // already check the validity of voltReturnType
        VoltType voltReturnType = VoltType.typeFromClass(methods.get("end").getReturnType());

        VoltXMLElement funcXML = new VoltXMLElement("ud_function")
                                    .withValue("name", functionName)
                                    .withValue("className", className)
                                    .withValue("returnType", String.valueOf(voltReturnType.getValue()));

        // here for current case, the assemble method has only 1 argument
        Class<?>[] paramTypeClasses = methods.get("assemble").getParameterTypes();
        VoltType[] voltParamTypes = new VoltType[paramTypeClasses.length];
        for (int i = 0; i < paramTypeClasses.length; i++) {
            // this loop only run once
            VoltType voltParamType = VoltType.typeFromClass(paramTypeClasses[i]);
            VoltXMLElement paramXML = new VoltXMLElement("udf_ptype")
                                         .withValue("type", String.valueOf(voltParamType.getValue()));
            funcXML.children.add(paramXML);
            voltParamTypes[i] = voltParamType;
        }
        // Register the function and get the function id.  This lets HSQL use the name in
        // indexes, materialized views and tuple limit delete statements.  These are not
        // valid, but it helps us give a nice error message.  Note that this definition
        // may revive a saved user defined function, and that nothing is put into the
        // catalog here.
        int functionId = FunctionForVoltDB.registerTokenForUDF(functionName,
                -1, voltReturnType, voltParamTypes, true);
        funcXML.withValue("functionid", String.valueOf(functionId));

        return funcXML;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
        throws VoltCompilerException, Error {
        // Matches if it is CREATE AGGREGATE FUNCTION <name> FROM CLASS <class-name>
        Matcher statementMatcher = SQLParser.matchCreateAggregateFunctionFromClass(ddlStatement.statement);
        if (!statementMatcher.matches()) {
            return false;
        }
        // Clean up the names
        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement).toLowerCase();
        // Class name is case sensitive.
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        // get the short name of the agg class
        String shortName = ProcedureCompiler.deriveShortProcedureName(className);
        // check the validity of loaded class
        Class<?> funcClass = isSQLParamValid(functionName, className, shortName);
        // find the four(start, assemble, combine, end) functions in the class
        // the methods are stored in a hashmap with their fuction name as the key
        // for example, methods.get("assemble") gives the assmble method
        // use VoltUDAggregate interface ensures the fours method must exist
        Map<String, Method> methods = retrieveMethodsFromClass(funcClass);

        VoltXMLElement funcXML = createVoltXMLElementForUDAF(functionName, className, methods);
        m_logger.debug(String.format("Added XML for function \"%s\"", functionName));
        m_schema.children.add(funcXML);
        return true;
    }
}
