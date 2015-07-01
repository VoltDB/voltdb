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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.Collection;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.store.BitMap;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of specific routine
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 1.9.0
 * @since 1.9.0
 */
public class Routine implements SchemaObject {

    public final static int NO_SQL       = 1;
    public final static int CONTAINS_SQL = 2;
    public final static int READS_SQL    = 3;
    public final static int MODIFIES_SQL = 4;

    //
    public final static int LANGUAGE_JAVA = 1;
    public final static int LANGUAGE_SQL  = 2;

    //
    public static final int PARAM_STYLE_JAVA = 1;
    public static final int PARAM_STYLE_SQL  = 2;

    //
    final static Routine[] emptyArray = new Routine[]{};

    //
    private HsqlName name;
    private HsqlName specificName;
    Type[]           parameterTypes;
    boolean[]        parameterNullable;
    int              typeGroups;
    Type             returnType;
    Type[]           tableType;
    TableDerived     returnTable;
    final int        routineType;
    int              language   = LANGUAGE_SQL;
    int              dataImpact = CONTAINS_SQL;
    int              parameterStyle;
    boolean          isDeterministic;
    boolean          isNullInputOutput;
    boolean          isNewSavepointLevel = true;
    boolean          isPSM;
    boolean          returnsTable;
    Statement        statement;

    //
    private String  methodName;
    Method          javaMethod;
    boolean         javaMethodWithConnection;
    private boolean isLibraryRoutine;

    //
    HashMappedList parameterList = new HashMappedList();
    int            scopeVariableCount;
    RangeVariable[] ranges = new RangeVariable[]{
        new RangeVariable(parameterList, false) };

    //
    int variableCount;

    //
    public Routine(int type) {
        routineType = type;
        returnType  = Type.SQL_ALL_TYPES;
    }

    public int getType() {
        return routineType;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ');

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
            sb.append(returnType.getTypeDefinition());
            sb.append(' ');
        }

        // SPECIFIC
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
        }

        if (language == LANGUAGE_JAVA) {
            sb.append(Tokens.T_EXTERNAL).append(' ').append(Tokens.T_NAME);
            sb.append(' ').append('\'').append(methodName).append('\'').append(
                ';');
        } else {
            sb.append(statement.getSQL());
        }

        return sb.toString();
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
        isPSM    = language == LANGUAGE_SQL;
    }

    public int getLanguage() {
        return language;
    }

    boolean isPSM() {
        return isPSM;
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

    public void setProcedure(Statement statement) {
        this.statement = statement;
    }

    public Statement getProcedure() {
        return statement;
    }

    public void setSpecificName(HsqlName name) {
        specificName = name;
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
    }

    public boolean returnsTable() {
        return returnsTable;
    }

    public void resolve() {

        if (this.routineType == SchemaObject.PROCEDURE && isNewSavepointLevel
                && dataImpact != MODIFIES_SQL) {
            throw Error.error(ErrorCode.X_42604);
        }

        setLanguage(language);

        if (language == Routine.LANGUAGE_SQL) {
            if (dataImpact == NO_SQL) {
                throw Error.error(ErrorCode.X_42604);
            }

            if (parameterStyle == PARAM_STYLE_JAVA) {
                throw Error.error(ErrorCode.X_42604);
            }
        }

        if (language == Routine.LANGUAGE_SQL) {
            if (parameterStyle != 0 && parameterStyle != PARAM_STYLE_SQL) {
                throw Error.error(ErrorCode.X_42604);
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

        if (statement != null) {
            statement.resolve();
        }

        if (methodName != null && javaMethod == null) {
            parameterNullable = new boolean[parameterTypes.length];

            boolean[] hasConnection = new boolean[1];

            javaMethod = getMethod(methodName, parameterTypes, returnType,
                                   parameterNullable, hasConnection);

            if (javaMethod == null) {
                throw Error.error(ErrorCode.X_46103);
            }

            javaMethodWithConnection = hasConnection[0];
        }
    }

    public boolean isProcedure() {
        return routineType == SchemaObject.PROCEDURE;
    }

    public boolean isFunction() {
        return routineType == SchemaObject.FUNCTION;
    }

    ColumnSchema getParameter(int i) {
        return (ColumnSchema) parameterList.get(i);
    }

    Type[] getParameterTypes() {
        return parameterTypes;
    }

    int getParameterSignature() {
        return typeGroups;
    }

    int getParameterCount() {
        return parameterTypes.length;
    }

    public int getParameterIndex(String name) {
        return parameterList.getIndex(name);
    }

    public RangeVariable[] getParameterRangeVariables() {
        return ranges;
    }

    public int getVariableCount() {
        return variableCount;
    }

    static Method getMethod(String name, Type[] types, Type returnType,
                            boolean[] nullability, boolean[] hasConnection) {

        int i = name.indexOf(':');

        if (i != -1) {
            if (!name.substring(0, i).equals(SqlInvariants.CLASSPATH_NAME)) {
                throw Error.error(ErrorCode.X_46102, name);
            }

            name = name.substring(i + 1);
        }

        Method   method  = null;
        Method[] methods = getMethods(name);

        for (i = 0; i < methods.length; i++) {
            int     offset = 0;
            Class[] params = methods[i].getParameterTypes();

            if (params.length > 0
                    && params[0].equals(java.sql.Connection.class)) {
                offset           = 1;
                hasConnection[0] = true;
            }

            if (params.length - offset != types.length) {
                continue;
            }

            Type methodReturnType = Type.getDefaultType(
                Types.getParameterSQLTypeNumber(methods[i].getReturnType()));

            if (methodReturnType == null) {
                continue;
            }

            if (methodReturnType.typeCode != returnType.typeCode) {
                continue;
            }

            method = methods[i];

            for (int j = 0; j < types.length; j++) {
                Class param = params[j + offset];
                Type methodParamType = Type.getDefaultType(
                    Types.getParameterSQLTypeNumber(param));

                if (methodParamType == null) {
                    break;
                }

                nullability[j] = !param.isPrimitive();

                if (types[j].typeCode != methodParamType.typeCode) {
                    method = null;

                    break;
                }
            }

            if (method != null) {
                break;
            }
        }

        return method;
    }

    static Method[] getMethods(String name) {

        int i = name.lastIndexOf('.');

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        String classname     = name.substring(0, i);
        String methodname    = name.substring(i + 1);
        Class  classinstance = null;

        try {
            classinstance = Class.forName(classname);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_42501, ErrorCode.M_Message_Pair,
                              new Object[] {
                classname, e
            });
        }

        Method[]      methods = classinstance.getMethods();
        HsqlArrayList list    = new HsqlArrayList();

        for (i = 0; i < methods.length; i++) {
            int    offset    = 0;
            Method m         = methods[i];
            int    modifiers = m.getModifiers();

            if (!m.getName().equals(methodname)
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
                Type methodParamType = Type.getDefaultTypeWithSize(
                    Types.getParameterSQLTypeNumber(param));

                if (methodParamType == null) {
                    m = null;

                    break;
                }
            }

            if (m == null) {
                continue;
            }

            Type methodReturnType = Type.getDefaultTypeWithSize(
                Types.getParameterSQLTypeNumber(m.getReturnType()));

            if (methodReturnType != null) {
                list.add(methods[i]);
            }
        }

        methods = new Method[list.size()];

        list.toArray(methods);

        return methods;
    }

    public static Routine[] newRoutines(Method[] methods) {

        Routine[] routines = new Routine[methods.length];

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            routines[i] = newRoutine(method);
        }

        return routines;
    }

    /**
     * Returns a new function Routine object based solely on a Java Method object.
     */
    public static Routine newRoutine(Method method) {

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

        if (className.equals("org.hsqldb_voltpatches.Library")
                || className.equals("java.lang.Math")) {
            routine.isLibraryRoutine = true;
        }

        for (int j = offset; j < params.length; j++) {
            Type methodParamType = Type.getDefaultTypeWithSize(
                Types.getParameterSQLTypeNumber(params[j]));
            ColumnSchema param = new ColumnSchema(null, methodParamType,
                                                  !params[j].isPrimitive(),
                                                  false, null);

            routine.addParameter(param);
        }

        routine.setLanguage(Routine.LANGUAGE_JAVA);
        routine.setMethod(method);
        routine.setMethodURL(name);
        routine.setDataImpact(Routine.NO_SQL);

        Type methodReturnType = Type.getDefaultTypeWithSize(
            Types.getParameterSQLTypeNumber(method.getReturnType()));

        routine.javaMethodWithConnection = offset == 1;;

        routine.setReturnType(methodReturnType);
        routine.resolve();

        return routine;
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
}
