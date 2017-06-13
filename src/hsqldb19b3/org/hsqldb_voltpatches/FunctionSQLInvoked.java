/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.store.ValuePool;

import java.lang.reflect.InvocationTargetException;

/**
 * Implementation of SQL-invoked user-defined function calls - PSM and JRT
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FunctionSQLInvoked extends Expression {

    RoutineSchema routineSchema;
    Routine       routine;

    FunctionSQLInvoked(RoutineSchema routineSchema) {

        super(OpTypes.FUNCTION);

        this.routineSchema = routineSchema;
    }

    public void setArguments(Expression[] newNodes) {
        this.nodes = newNodes;
    }

    public void resolveTypes(Session session, Expression parent) {

        Type[] types = new Type[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];

            e.resolveTypes(session, this);

            types[i] = e.dataType;
        }

        routine = routineSchema.getSpecificRoutine(types);

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].dataType == null) {
                nodes[i].dataType = routine.getParameterTypes()[i];
            }
        }

        dataType = routine.getReturnType();
    }

    public Object getValue(Session session) {

        int      variableCount = routine.getVariableCount();
        Result   result;
        int      extraArg    = routine.javaMethodWithConnection ? 1
                                                                : 0;
        Object[] data        = ValuePool.emptyObjectArray;
        Object   returnValue = null;
        boolean push = routine.isPSM() || routine.dataImpact != Routine.NO_SQL;

        if (extraArg + nodes.length > 0) {
            data = new Object[nodes.length + extraArg];

            if (extraArg > 0) {
                data[0] = session.getInternalConnection();
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            Expression e     = nodes[i];
            Object     value = e.getValue(session, e.dataType);

            if (value == null) {
                if (routine.isNullInputOutput()) {
                    return null;
                }

                if (!routine.parameterNullable[i]) {
                    throw Error.error(ErrorCode.X_39004);
                }
            }

            if (routine.isPSM()) {
                data[i] = value;
            } else {
                data[i + extraArg] = e.dataType.convertSQLToJava(session,
                        value);
            }
        }

        if (push) {
            session.sessionContext.push();
        }

        if (routine.isPSM()) {
            session.sessionContext.routineArguments = data;
            session.sessionContext.routineVariables =
                ValuePool.emptyObjectArray;

            if (variableCount > 0) {
                session.sessionContext.routineVariables =
                    new Object[variableCount];
            }

            result = routine.statement.execute(session);

            if (result.isError()) {}
            else if (result.isSimpleValue()) {
                returnValue = result.getValueObject();
            } else {
                result = Result.newErrorResult(
                    Error.error(ErrorCode.X_2F005, routine.getName().name),
                    null);
            }
        } else {
            try {
                returnValue = routine.javaMethod.invoke(null, data);

                if (routine.returnsTable()) {

                    // convert ResultSet to table
                } else {
                    returnValue = dataType.convertJavaToSQL(session,
                            returnValue);
                }

                result = Result.updateZeroResult;
            } catch (InvocationTargetException e) {
                result = Result.newErrorResult(
                    Error.error(ErrorCode.X_46000, routine.getName().name),
                    null);
            } catch (IllegalAccessException e) {
                result = Result.newErrorResult(
                    Error.error(ErrorCode.X_46000, routine.getName().name),
                    null);
            } catch (Throwable e) {
                result = Result.newErrorResult(
                    Error.error(ErrorCode.X_46000, routine.getName().name),
                    null);
            }
        }

        if (push) {
            if (result.isError()) {
                session.rollbackToSavepoint();
            }

            session.sessionContext.pop();
        }

        if (result.isError()) {
            throw result.getException();
        }

        return returnValue;
    }

    public String getSQL() {
        return Tokens.T_FUNCTION;
    }

    public String describe(Session session) {
        return super.describe(session);
    }

    /************************* Volt DB Extensions *************************/
    // When we start to use it, we need to override equals and hashCode function

    /**********************************************************************/
}
