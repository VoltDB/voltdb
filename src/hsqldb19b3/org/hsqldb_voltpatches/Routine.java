/* Copyright (c) 2001-2014, The HSQL Development Group
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.jdbc.JDBCResultSet;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.map.BitMap;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.RowType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of specific routine
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.3.0
 * @since 1.9.0
 */
public class Routine implements SchemaObject, RangeGroup, Cloneable {

    public static final int NO_SQL       = 1;
    public static final int CONTAINS_SQL = 2;
    public static final int READS_SQL    = 3;
    public static final int MODIFIES_SQL = 4;

    //
    public static final int LANGUAGE_JAVA = 1;
    public static final int LANGUAGE_SQL  = 2;

    //
    public static final int PARAM_STYLE_JAVA = 1;
    public static final int PARAM_STYLE_SQL  = 2;

    //
    static final Routine[] emptyArray = new Routine[]{};

    //
    RoutineSchema    routineSchema;
    private HsqlName name;
    private HsqlName specificName;
    Type[]           parameterTypes;
    int              typeGroups;
    Type             returnType;
    Type[]           tableType;
    Table            returnTable;
    final int        routineType;
    int              language   = LANGUAGE_SQL;
    int              dataImpact = CONTAINS_SQL;
    int              parameterStyle;
    boolean          isDeterministic;
    boolean          isNullInputOutput;
    boolean          isNewSavepointLevel = true;
    int              maxDynamicResults   = 0;
    boolean          isRecursive;
    boolean          returnsTable;
    Statement        statement;

    //
    boolean isAggregate;
    boolean isIndex;
    boolean isSearch;

    //
    private String  methodName;
    Method          javaMethod;
    boolean         javaMethodWithConnection;
    private boolean isLibraryRoutine;

    //
    HashMappedList  parameterList = new HashMappedList();
    RangeVariable[] ranges        = RangeVariable.emptyArray;

    //
    int variableCount;

    //
    OrderedHashSet references;

    //
    Table triggerTable;
    int   triggerType;
    int   triggerOperation;

    public Routine(int type) {

        routineType = type;
        returnType  = Type.SQL_ALL_TYPES;
        ranges = new RangeVariable[]{
            new RangeVariable(parameterList, null, false,
                              RangeVariable.PARAMETER_RANGE) };
    }

    public Routine(Table table, RangeVariable[] ranges, int impact,
                   int triggerType, int operationType) {

        routineType           = SchemaObject.TRIGGER;
        returnType            = Type.SQL_ALL_TYPES;
        dataImpact            = impact;
        this.ranges           = ranges;
        this.triggerTable     = table;
        this.triggerType      = triggerType;
        this.triggerOperation = operationType;
    }

    public int getType() {
        return routineType;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {

        if (routineType == SchemaObject.TRIGGER) {
            return triggerTable.getSchemaName();
        }

        return name.schema;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {

        ParserRoutine p = new ParserRoutine(session,
                                            new Scanner(statement.getSQL()));

        p.read();
        p.startRecording();

        Statement statement = p.compileSQLProcedureStatementOrNull(this, null);
        Token[]   tokenisedStatement = p.getRecordedStatement();
        String    sql                = Token.getSQL(tokenisedStatement);

        statement.setSQL(sql);
        setProcedure(statement);
        statement.resolve(session);
        setReferences();
    }

    public String getSQL() {
        return getDefinitionSQL(true);
    }

    public String getSQLAlter() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ALTER).append(' ').append(Tokens.T_SPECIFIC);
        sb.append(' ').append(Tokens.T_ROUTINE).append(' ');
        sb.append(specificName.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_BODY);
        sb.append(' ').append(statement.getSQL());

        return sb.toString();
    }

    public String getSQLDeclaration() {
        return getDefinitionSQL(false);
    }

    private String getDefinitionSQL(boolean withBody) {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ');

        if (isAggregate) {
            sb.append(Tokens.T_AGGREGATE).append(' ');
        }

        if (routineType == SchemaObject.PROCEDURE) {
            sb.append(Tokens.T_PROCEDURE);
        } else {
            sb.append(Tokens.T_FUNCTION);
        }

        sb.append(' ');
        sb.append(name.getSchemaQualifiedStatementName());
        sb.append('(');

        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }

            ColumnSchema param = (ColumnSchema) parameterList.get(i);

            // in - out
            sb.append(param.getSQL());
        }

        sb.append(')');
        sb.append(' ');

        if (routineType == SchemaObject.FUNCTION) {
            sb.append(Tokens.T_RETURNS);
            sb.append(' ');

            if (returnsTable) {
                sb.append(Tokens.T_TABLE);
                sb.append(returnTable.getColumnListWithTypeSQL());
            } else {
                sb.append(returnType.getTypeDefinition());
            }

            sb.append(' ');
        }

        // SPECIFIC
        if (specificName != null) {
            sb.append(Tokens.T_SPECIFIC);
            sb.append(' ');
            sb.append(specificName.getStatementName());
            sb.append(' ');
        }

        //
        sb.append(Tokens.T_LANGUAGE);
        sb.append(' ');

        if (language == LANGUAGE_JAVA) {
            sb.append(Tokens.T_JAVA);
        } else {
            sb.append(Tokens.T_SQL);
        }

        sb.append(' ');

        //
        if (!isDeterministic) {
            sb.append(Tokens.T_NOT);
            sb.append(' ');
        }

        sb.append(Tokens.T_DETERMINISTIC);
        sb.append(' ');

        //
        sb.append(getDataImpactString());
        sb.append(' ');

        //
        if (routineType == SchemaObject.FUNCTION) {
            if (isNullInputOutput) {
                sb.append(Tokens.T_RETURNS).append(' ').append(Tokens.T_NULL);
            } else {
                sb.append(Tokens.T_CALLED);
            }

            sb.append(' ').append(Tokens.T_ON).append(' ');
            sb.append(Tokens.T_NULL).append(' ').append(Tokens.T_INPUT);
            sb.append(' ');
        } else {
            if (isNewSavepointLevel) {
                sb.append(Tokens.T_NEW);
            } else {
                sb.append(Tokens.T_OLD);
            }

            sb.append(' ').append(Tokens.T_SAVEPOINT).append(' ');
            sb.append(Tokens.T_LEVEL).append(' ');

            if (maxDynamicResults != 0) {
                sb.append(' ').append(Tokens.T_DYNAMIC).append(' ');
                sb.append(Tokens.T_RESULT).append(' ').append(Tokens.T_SETS);
                sb.append(' ').append(maxDynamicResults).append(' ');
            }
        }

        if (language == LANGUAGE_JAVA) {
            sb.append(Tokens.T_EXTERNAL).append(' ').append(Tokens.T_NAME);
            sb.append(' ').append('\'').append(methodName).append('\'');
        } else {
            if (withBody) {
                sb.append(statement.getSQL());
            } else {
                sb.append(Tokens.T_SIGNAL).append(' ');
                sb.append(Tokens.T_SQLSTATE).append(' ');
                sb.append('\'').append("45000").append('\'');
            }
        }

        return sb.toString();
    }

    public String getSQLBodyDefinition() {

        StringBuffer sb = new StringBuffer();

        if (language == LANGUAGE_JAVA) {
            sb.append(Tokens.T_EXTERNAL).append(' ').append(Tokens.T_NAME);
            sb.append(' ').append('\'').append(methodName).append('\'');
        } else {
            sb.append(statement.getSQL());
        }

        return sb.toString();
    }

    public String getExternalName() {

        if (language == LANGUAGE_JAVA) {
            return methodName;
        } else {
            return null;
        }
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public void addParameter(ColumnSchema param) {

        HsqlName name = param.getName();
        String paramName =
            name == null
            ? HsqlNameManager.getAutoNoNameColumnString(parameterList.size())
            : name.name;

        parameterList.add(paramName, param);
    }

    public void setLanguage(int lang) {
        language = lang;
    }

    public int getLanguage() {
        return language;
    }

    boolean isPSM() {
        return language == LANGUAGE_SQL;
    }

    public void setDataImpact(int impact) {
        dataImpact = impact;
    }

    public int getDataImpact() {
        return dataImpact;
    }

    public String getDataImpactString() {

        StringBuffer sb = new StringBuffer();

        switch (this.dataImpact) {

            case NO_SQL :
                sb.append(Tokens.T_NO).append(' ').append(Tokens.T_SQL);
                break;

            case CONTAINS_SQL :
                sb.append(Tokens.T_CONTAINS).append(' ').append(Tokens.T_SQL);
                break;

            case READS_SQL :
                sb.append(Tokens.T_READS).append(' ').append(
                    Tokens.T_SQL).append(' ').append(Tokens.T_DATA);
                break;

            case MODIFIES_SQL :
                sb.append(Tokens.T_MODIFIES).append(' ').append(
                    Tokens.T_SQL).append(' ').append(Tokens.T_DATA);
                break;
        }

        return sb.toString();
    }

    public void setReturnType(Type type) {
        returnType = type;
    }

    public Type getReturnType() {
        return returnType;
    }

    public void setTableType(Type[] types) {
        tableType = types;
    }

    public Type[] getTableType() {
        return tableType;
    }

    public Table getTable() {
        return returnTable;
    }

    public void setProcedure(Statement statement) {
        this.statement = statement;
    }

    public Statement getProcedure() {
        return statement;
    }

    public void setSpecificName(HsqlName name) {
        specificName = name;
    }

    public int getMaxDynamicResults() {
        return maxDynamicResults;
    }

    public void setName(HsqlName name) {
        this.name = name;
    }

    public HsqlName getSpecificName() {
        return specificName;
    }

    public void setDeterministic(boolean value) {
        isDeterministic = value;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public void setNullInputOutput(boolean value) {
        isNullInputOutput = value;
    }

    public boolean isNullInputOutput() {
        return isNullInputOutput;
    }

    public void setNewSavepointLevel(boolean value) {
        isNewSavepointLevel = value;
    }

    public void setMaxDynamicResults(int value) {
        maxDynamicResults = value;
    }

    public void setParameterStyle(int style) {
        parameterStyle = style;
    }

    public void setMethodURL(String url) {
        this.methodName = url;
    }

    public Method getMethod() {
        return javaMethod;
    }

    public void setMethod(Method method) {
        this.javaMethod = method;
    }

    public void setReturnTable(TableDerived table) {

        this.returnTable  = table;
        this.returnsTable = true;

        SimpleName[] names = new SimpleName[table.getColumnCount()];
        Type[]       types = table.getColumnTypes();

        returnType = new RowType(types);
    }

    public boolean returnsTable() {
        return returnsTable;
    }

    public void setAggregate(boolean isAggregate) {
        this.isAggregate = isAggregate;
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    public void resolve(Session session) {

        setLanguage(language);

        if (language == Routine.LANGUAGE_SQL) {
            if (dataImpact == NO_SQL) {
                throw Error.error(ErrorCode.X_42604, "CONTAINS SQL");
            }

            if (parameterStyle == PARAM_STYLE_JAVA) {
                throw Error.error(ErrorCode.X_42604, "PARAMETER STYLE");
            }
        }

        if (language == Routine.LANGUAGE_SQL) {
            if (parameterStyle != 0 && parameterStyle != PARAM_STYLE_SQL) {
                throw Error.error(ErrorCode.X_42604, "PARAMETER STYLE");
            }
        }

        parameterTypes = new Type[parameterList.size()];
        typeGroups     = 0;

        for (int i = 0; i < parameterTypes.length; i++) {
            ColumnSchema param = (ColumnSchema) parameterList.get(i);

            parameterTypes[i] = param.dataType;

            if (i < 4) {
                BitMap.setByte(typeGroups,
                               (byte) param.dataType.typeComparisonGroup,
                               i * 8);
            }
        }

        if (isAggregate) {
            if (parameterTypes.length != 4) {
                throw Error.error(ErrorCode.X_42610);
            }

            boolean check = parameterTypes[1].typeCode == Types.BOOLEAN;

            //
            ColumnSchema param = (ColumnSchema) parameterList.get(0);

            check &= param.getParameterMode()
                     == SchemaObject.ParameterModes.PARAM_IN;
            param = (ColumnSchema) parameterList.get(1);
            check &= param.getParameterMode()
                     == SchemaObject.ParameterModes.PARAM_IN;
            param = (ColumnSchema) parameterList.get(2);
            check &= param.getParameterMode()
                     == SchemaObject.ParameterModes.PARAM_INOUT;
            param = (ColumnSchema) parameterList.get(3);
            check &= param.getParameterMode()
                     == SchemaObject.ParameterModes.PARAM_INOUT;

            if (!check) {
                throw Error.error(ErrorCode.X_42610);
            }
        }

        resolveReferences(session);
    }

    void resolveReferences(Session session) {

        if (statement != null) {
            statement.resolve(session);
            checkSQLData(session);
        }

        if (methodName != null && javaMethod == null) {
            boolean[] hasConnection = new boolean[1];

            javaMethod = getMethod(methodName, this, hasConnection,
                                   returnsTable);

            if (javaMethod == null) {
                throw Error.error(ErrorCode.X_46103);
            }

            javaMethodWithConnection = hasConnection[0];

            String className = javaMethod.getDeclaringClass().getName();

            if (className.equals("java.lang.Math")) {
                isLibraryRoutine = true;
            }
        }

        setReferences();
    }

    private void setReferences() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < parameterTypes.length; i++) {
            ColumnSchema   param = (ColumnSchema) parameterList.get(i);
            OrderedHashSet refs  = param.getReferences();

            if (refs != null) {
                set.addAll(refs);
            }
        }

        if (statement != null) {
            set.addAll(statement.getReferences());
        }

        isRecursive = false;

        if (set.contains(getSpecificName())) {
            set.remove(this.getSpecificName());

            isRecursive = true;
        }

        references = set;
    }

    void checkSQLData(Session session) {

        OrderedHashSet set = statement.getReferences();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            if (name.type == SchemaObject.SPECIFIC_ROUTINE) {
                Routine routine =
                    (Routine) session.database.schemaManager.getSchemaObject(
                        name);

                if (routine.dataImpact == Routine.READS_SQL) {
                    if (dataImpact == Routine.CONTAINS_SQL) {
                        throw Error.error(ErrorCode.X_42608,
                                          Tokens.T_READS + ' ' + Tokens.T_SQL);
                    }
                } else if (routine.dataImpact == Routine.MODIFIES_SQL) {
                    if (dataImpact == Routine.CONTAINS_SQL
                            || dataImpact == Routine.READS_SQL) {
                        throw Error.error(ErrorCode.X_42608,
                                          Tokens.T_MODIFIES + ' '
                                          + Tokens.T_SQL);
                    }
                }
            }
        }

        if (dataImpact == Routine.CONTAINS_SQL
                || dataImpact == Routine.READS_SQL) {
            HsqlName[] names = statement.getTableNamesForWrite();

            for (int i = 0; i < names.length; i++) {
                if (names[i].schema != SqlInvariants.MODULE_HSQLNAME) {
                    throw Error.error(ErrorCode.X_42608,
                                      Tokens.T_MODIFIES + ' ' + Tokens.T_SQL);
                }
            }
        }

        if (dataImpact == Routine.CONTAINS_SQL) {
            HsqlName[] names = statement.getTableNamesForRead();

            for (int i = 0; i < names.length; i++) {
                if (names[i].schema != SqlInvariants.MODULE_HSQLNAME) {
                    throw Error.error(ErrorCode.X_42608,
                                      Tokens.T_READS + ' ' + Tokens.T_SQL);
                }
            }
        }
    }

    public boolean isTrigger() {
        return routineType == SchemaObject.TRIGGER;
    }

    public boolean isProcedure() {
        return routineType == SchemaObject.PROCEDURE;
    }

    public boolean isFunction() {
        return routineType == SchemaObject.FUNCTION;
    }

    public ColumnSchema getParameter(int i) {
        return (ColumnSchema) parameterList.get(i);
    }

    Type[] getParameterTypes() {
        return parameterTypes;
    }

    int getParameterSignature() {
        return typeGroups;
    }

    public int getParameterCount() {
        return parameterTypes.length;
    }

    public int getParameterCount(int type) {

        int count = 0;

        for (int i = 0; i < parameterList.size(); i++) {
            ColumnSchema col = (ColumnSchema) parameterList.get(i);

            if (col.getParameterMode() == type) {
                count++;
            }
        }

        return count;
    }

    public int getParameterIndex(String name) {
        return parameterList.getIndex(name);
    }

    public RangeVariable[] getRangeVariables() {
        return ranges;
    }

    public void setCorrelated() {

        //
    }

    public boolean isVariable() {
        return true;
    }

    public int getVariableCount() {
        return variableCount;
    }

    public boolean isLibraryRoutine() {
        return isLibraryRoutine;
    }

    public HsqlName[] getTableNamesForRead() {

        if (statement == null) {
            return HsqlName.emptyArray;
        }

        return statement.getTableNamesForRead();
    }

    public HsqlName[] getTableNamesForWrite() {

        if (statement == null) {
            return HsqlName.emptyArray;
        }

        return statement.getTableNamesForWrite();
    }

    public void resetAlteredRoutineSettings() {

        if (isPSM()) {
            methodName               = null;
            javaMethod               = null;
            javaMethodWithConnection = false;
            parameterStyle           = PARAM_STYLE_SQL;

            if (dataImpact == NO_SQL) {
                dataImpact = CONTAINS_SQL;
            }
        } else {
            statement     = null;
            references    = null;
            variableCount = 0;
            ranges        = RangeVariable.emptyArray;
        }
    }

    public void setAsAlteredRoutine(Routine routine) {

        language          = routine.language;
        dataImpact        = routine.dataImpact;
        parameterStyle    = routine.parameterStyle;
        isDeterministic   = routine.isDeterministic;
        isNullInputOutput = routine.isNullInputOutput;
        maxDynamicResults = routine.maxDynamicResults;
        isRecursive       = routine.isRecursive;
        javaMethod        = routine.javaMethod;

        //
        isRecursive              = routine.isRecursive;
        javaMethodWithConnection = routine.javaMethodWithConnection;
        methodName               = routine.methodName;
        statement                = routine.statement;
        references               = routine.references;
        variableCount            = routine.variableCount;
        ranges                   = routine.ranges;
    }

    Object[] convertArgsToJava(Session session, Object[] callArguments) {

        int      extraArg = javaMethodWithConnection ? 1
                                                     : 0;
        Object[] data     = new Object[javaMethod.getParameterTypes().length];
        Type[]   types    = getParameterTypes();
        int      i        = 0;

        for (; i < types.length; i++) {
            Object       value = callArguments[i];
            ColumnSchema param = getParameter(i);

            if (param.parameterMode == SchemaObject.ParameterModes.PARAM_IN) {
                data[i + extraArg] = types[i].convertSQLToJava(session, value);
            } else {
                Object jdbcValue = types[i].convertSQLToJava(session, value);
                Class  cl        = types[i].getJDBCClass();
                Object array     = java.lang.reflect.Array.newInstance(cl, 1);

                java.lang.reflect.Array.set(array, 0, jdbcValue);

                data[i + extraArg] = array;
            }
        }

        for (; i + extraArg < data.length; i++) {
            data[i + extraArg] = new java.sql.ResultSet[1];
        }

        return data;
    }

    void convertArgsToSQL(Session session, Object[] callArguments,
                          Object[] data) {

        int    extraArg = javaMethodWithConnection ? 1
                                                   : 0;
        Type[] types    = getParameterTypes();
        int    i        = 0;

        for (; i < types.length; i++) {
            Object       value = data[i + extraArg];
            ColumnSchema param = getParameter(i);

            if (param.parameterMode != SchemaObject.ParameterModes.PARAM_IN) {
                value = java.lang.reflect.Array.get(value, 0);
            }

            callArguments[i] = types[i].convertJavaToSQL(session, value);
        }

        Result head = null;

        for (; i + extraArg < data.length; i++) {
            ResultSet rs = ((ResultSet[]) data[i + extraArg])[0];

            if (rs != null) {
                if (rs instanceof JDBCResultSet) {
                    Result r = ((JDBCResultSet) rs).result;

                    if (head == null) {
                        callArguments[i] = r;
                        head             = r;
                    } else {
                        head.addChainedResult(r);
                    }
                } else {
                    Error.error(ErrorCode.X_46000, "ResultSet not native");
                }
            }
        }
    }

    public Result invokeJavaMethodDirect(Object[] data) {

        Result result;

        try {
            Object returnValue = javaMethod.invoke(null, data);

            returnValue = returnType.convertJavaToSQL(null, returnValue);
            result      = Result.newPSMResult(returnValue);
        } catch (Throwable t) {
            result = Result.newErrorResult(
                Error.error(t, ErrorCode.X_46000, getName().name), null);
        }

        return result;
    }

    Result invokeJavaMethod(Session session, Object[] data) {

        Result   result;
        HsqlName oldSessionSchema = session.getCurrentSchemaHsqlName();

        try {
            if (dataImpact == Routine.NO_SQL) {
                session.sessionContext.isReadOnly = Boolean.TRUE;

                session.setNoSQL();
            } else if (dataImpact == Routine.CONTAINS_SQL) {
                session.sessionContext.isReadOnly = Boolean.TRUE;
            } else if (dataImpact == Routine.READS_SQL) {
                session.sessionContext.isReadOnly = Boolean.TRUE;
            }

            session.setCurrentSchemaHsqlName(getSchemaName());

            Object returnValue = javaMethod.invoke(null, data);

            if (returnsTable()) {
                if (returnValue instanceof JDBCResultSet) {
                    result = ((JDBCResultSet) returnValue).result;
                } else {

                    // convert ResultSet to table
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "FunctionSQLInvoked");
                }
            } else {
                returnValue = returnType.convertJavaToSQL(session,
                        returnValue);
                result = Result.newPSMResult(returnValue);
            }
        } catch (InvocationTargetException e) {
            result = Result.newErrorResult(
                Error.error(e, ErrorCode.X_46000, getName().name), null);
        } catch (IllegalAccessException e) {
            result = Result.newErrorResult(
                Error.error(e, ErrorCode.X_46000, getName().name), null);
        } catch (Throwable e) {
            result = Result.newErrorResult(
                Error.error(e, ErrorCode.X_46000, getName().name), null);
        }

        session.setCurrentSchemaHsqlName(oldSessionSchema);

        return result;
    }

    public Result invoke(Session session, Object[] data,
                         Object[] aggregateData, boolean push) {

        Result result;

        if (push) {
            session.sessionContext.pushRoutineInvocation();
        }

        if (isPSM()) {
            try {
                session.sessionContext.routineArguments = data;
                session.sessionContext.routineVariables =
                    ValuePool.emptyObjectArray;

                if (variableCount > 0) {
                    session.sessionContext.routineVariables =
                        new Object[variableCount];
                }

                result = statement.execute(session);

                if (aggregateData != null) {
                    for (int i = 0; i < aggregateData.length; i++) {
                        aggregateData[i] = data[i + 1];
                    }
                }
            } catch (Throwable e) {
                result = Result.newErrorResult(e);
            }
        } else {
            if (isAggregate) {
                data = convertArgsToJava(session, data);
            }

            result = invokeJavaMethod(session, data);

            if (isAggregate) {
                Object[] callResult = new Object[data.length];

                convertArgsToSQL(session, callResult, data);

                for (int i = 0; i < aggregateData.length; i++) {
                    aggregateData[i] = callResult[i + 1];
                }
            }
        }

        if (push) {
            session.sessionContext.popRoutineInvocation();
        }

        return result;
    }

    public Routine duplicate() {

        try {
            return (Routine) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }
    }

    static Method getMethod(String name, Routine routine,
                            boolean[] hasConnection, boolean returnsTable) {

        int i = name.indexOf(':');

        if (i != -1) {
            if (!name.substring(0, i).equals(SqlInvariants.CLASSPATH_NAME)) {
                throw Error.error(ErrorCode.X_46102, name);
            }

            name = name.substring(i + 1);
        }

        Method[] methods       = getMethods(name);
        int      firstMismatch = -1;

        for (i = 0; i < methods.length; i++) {
            int offset = 0;

            hasConnection[0] = false;

            Method  method = methods[i];
            Class[] params = method.getParameterTypes();
            int     matchedParamCount;

            if (params.length > 0
                    && params[0].equals(java.sql.Connection.class)) {
                offset           = 1;
                hasConnection[0] = true;
            }

            matchedParamCount = params.length - offset;

            if (routine.isProcedure()) {
                for (int j = offset; j < params.length; j++) {
                    if (params[j].isArray()
                            && java.sql.ResultSet.class.isAssignableFrom(
                                params[j].getComponentType())) {
                        matchedParamCount = j - offset;

                        break;
                    }
                }
            }

            if (matchedParamCount != routine.parameterTypes.length) {
                continue;
            }

            if (returnsTable) {
                if (!java.sql.ResultSet.class.isAssignableFrom(
                        method.getReturnType())) {
                    continue;
                }
            } else {
                Type methodReturnType =
                    Types.getParameterSQLType(method.getReturnType());

                if (methodReturnType == null) {
                    continue;
                }

                if (!routine.returnType.canBeAssignedFrom(methodReturnType)) {
                    continue;
                }

                if (!methodReturnType.isLobType()
                        && (methodReturnType.isBinaryType()
                            || methodReturnType.isCharacterType())) {

                    //
                } else {
                    int routineRetType = routine.returnType.typeCode;

                    if (routineRetType == Types.SQL_NUMERIC) {
                        routineRetType = Types.SQL_DECIMAL;
                    }

                    if (methodReturnType.typeCode != routineRetType) {
                        continue;
                    }
                }
            }

            for (int j = 0; j < routine.parameterTypes.length; j++) {
                boolean isInOut = false;
                Class   param   = params[j + offset];

                if (param.isArray()) {
                    if (!byte[].class.equals(param)) {
                        param = param.getComponentType();

                        if (param.isPrimitive()) {
                            method = null;

                            break;
                        }

                        isInOut = true;
                    }
                }

                Type methodParamType = Types.getParameterSQLType(param);

                if (methodParamType == null) {
                    method = null;

                    break;
                }

                boolean result = routine.parameterTypes[j].typeComparisonGroup
                                 == methodParamType.typeComparisonGroup;

                // exact type for number
                if (result && routine.parameterTypes[j].isNumberType()) {
                    int routineParamType = routine.parameterTypes[j].typeCode;

                    if (routineParamType == Types.SQL_NUMERIC) {
                        routineParamType = Types.SQL_DECIMAL;
                    }

                    result = routineParamType == methodParamType.typeCode;
                }

                if (isInOut
                        && routine.getParameter(j).parameterMode
                           == SchemaObject.ParameterModes.PARAM_IN) {
                    result = false;
                }

                if (!result) {
                    method = null;

                    if (j + offset > firstMismatch) {
                        firstMismatch = j + offset;
                    }

                    break;
                }
            }

            if (method != null) {
                for (int j = 0; j < routine.parameterTypes.length; j++) {
                    routine.getParameter(j).setNullable(
                        !params[j + offset].isPrimitive());
                }

                return method;
            }
        }

        if (firstMismatch >= 0) {
            ColumnSchema param = routine.getParameter(firstMismatch);

            throw Error.error(ErrorCode.X_46511, param.getNameString());
        }

        return null;
    }

    static Method[] getMethods(String name) {

        int i = name.lastIndexOf('.');

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        String   className  = name.substring(0, i);
        String   methodname = name.substring(i + 1);
        Class    cl;
        Method[] methods = null;

        if (!HsqlDatabaseProperties.supportsJavaMethod(name)) {
            throw Error.error(ErrorCode.X_42501, className);
        }

        try {
            cl = Class.forName(className, true,
                               Thread.currentThread().getContextClassLoader());
        } catch (Throwable t1) {
            try {
                cl = Class.forName(className);
            } catch (Throwable t) {
                throw Error.error(t, ErrorCode.X_42501,
                                  ErrorCode.M_Message_Pair, new Object[] {
                    t.toString(), className
                });
            }
        }

        try {
            methods = cl.getMethods();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.X_42501, ErrorCode.M_Message_Pair,
                              new Object[] {
                t.toString(), className
            });
        }

        HsqlArrayList list = new HsqlArrayList();

        for (i = 0; i < methods.length; i++) {
            int    offset    = 0;
            int    endIndex  = Integer.MAX_VALUE;
            Method method    = methods[i];
            int    modifiers = method.getModifiers();

            if (!method.getName().equals(methodname)
                    || !Modifier.isStatic(modifiers)
                    || !Modifier.isPublic(modifiers)) {
                continue;
            }

            Class[] params = methods[i].getParameterTypes();

            if (params.length > 0
                    && params[0].equals(java.sql.Connection.class)) {
                offset = 1;
            }

            for (int j = offset; j < params.length; j++) {
                Class param = params[j];

                if (param.isArray()) {
                    if (!byte[].class.equals(param)) {
                        param = param.getComponentType();

                        if (param.isPrimitive()) {
                            method = null;

                            break;
                        }

                        if (java.sql.ResultSet.class.isAssignableFrom(param)) {
                            if (endIndex > j) {
                                endIndex = j;
                            }
                        }
                    }

                    if (j >= endIndex) {
                        if (java.sql.ResultSet.class.isAssignableFrom(param)) {
                            continue;
                        } else {
                            method = null;

                            break;
                        }
                    }
                } else {
                    if (j > endIndex) {
                        method = null;

                        break;
                    }
                }

                Type methodParamType = Types.getParameterSQLType(param);

                if (methodParamType == null) {
                    method = null;

                    break;
                }
            }

            if (method == null) {
                continue;
            }

            if (java.sql.ResultSet.class.isAssignableFrom(
                    method.getReturnType())) {
                list.add(methods[i]);
            } else {
                Type methodReturnType =
                    Types.getParameterSQLType(method.getReturnType());

                if (methodReturnType != null) {
                    list.add(methods[i]);
                }
            }
        }

        methods = new Method[list.size()];

        list.toArray(methods);

        return methods;
    }

    public static Routine[] newRoutines(Session session, Method[] methods) {

        Routine[] routines = new Routine[methods.length];

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            routines[i] = newRoutine(session, method);
        }

        return routines;
    }

    /**
     * Returns a new function Routine object based solely on a Java Method object.
     */
    public static Routine newRoutine(Session session, Method method) {

        Routine      routine   = new Routine(SchemaObject.FUNCTION);
        int          offset    = 0;
        Class[]      params    = method.getParameterTypes();
        String       className = method.getDeclaringClass().getName();
        StringBuffer sb        = new StringBuffer();

        sb.append("CLASSPATH:");
        sb.append(method.getDeclaringClass().getName()).append('.');
        sb.append(method.getName());

        if (params.length > 0 && params[0].equals(java.sql.Connection.class)) {
            offset = 1;
        }

        String name = sb.toString();

        if (className.equals("java.lang.Math")) {
            routine.isLibraryRoutine = true;
        }

        for (int j = offset; j < params.length; j++) {
            Type methodParamType = Types.getParameterSQLType(params[j]);
            HsqlName colName = session.database.nameManager.newHsqlName("C"
                + (j - offset + 1), false, SchemaObject.PARAMETER);
            ColumnSchema param = new ColumnSchema(colName, methodParamType,
                                                  !params[j].isPrimitive(),
                                                  false, null);

            routine.addParameter(param);
        }

        routine.setLanguage(Routine.LANGUAGE_JAVA);
        routine.setMethod(method);
        routine.setMethodURL(name);
        routine.setDataImpact(Routine.NO_SQL);

        Type methodReturnType =
            Types.getParameterSQLType(method.getReturnType());

        routine.javaMethodWithConnection = offset == 1;

        routine.setReturnType(methodReturnType);
        routine.resolve(session);

        return routine;
    }

    public static void createRoutines(Session session, HsqlName schema,
                                      String name) {

        Method[]  methods  = Routine.getMethods(name);
        Routine[] routines = Routine.newRoutines(session, methods);
        HsqlName routineName = session.database.nameManager.newHsqlName(schema,
            name, true, SchemaObject.FUNCTION);

        for (int i = 0; i < routines.length; i++) {
            routines[i].setName(routineName);
            session.database.schemaManager.addSchemaObject(routines[i]);
        }
    }
}
