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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.LongDeque;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.result.ResultProperties;
import org.hsqldb_voltpatches.types.ArrayType;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Parser for SQL stored procedures and functions - PSM
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.1
 * @since 1.9.0
 */
public class ParserRoutine extends ParserDML {

    ParserRoutine(Session session, Scanner t) {
        super(session, t);
    }

    /**
     *  Reads a DEFAULT clause expression.
     */
    /*
     for datetime, the default must have the same fields
     */
    Expression readDefaultClause(Type dataType) {

        Expression e     = null;
        boolean    minus = false;

        if (token.tokenType == Tokens.NULL) {
            read();

            return new ExpressionValue(null, dataType);
        }

        if (dataType.isDateTimeType() || dataType.isIntervalType()) {
            switch (token.tokenType) {

                case Tokens.DATE :
                case Tokens.TIME :
                case Tokens.TIMESTAMP :
                case Tokens.INTERVAL : {
                    e = readDateTimeIntervalLiteral(session);

                    if (e.dataType.typeCode != dataType.typeCode) {

                        // error message
                        throw unexpectedToken();
                    }

                    Object defaultValue = e.getValue(session, dataType);

                    return new ExpressionValue(defaultValue, dataType);
                }
                case Tokens.X_VALUE :
                    break;

                default :
                    e = XreadDateTimeValueFunctionOrNull();

                    if (e == null) {
                        break;
                    }

                    e = XreadModifier(e);
                    break;
            }
        } else if (dataType.isNumberType()) {
            if (token.tokenType == Tokens.MINUS) {
                read();

                minus = true;
            } else {
                if (database.sqlSyntaxPgs
                        && token.tokenType == Tokens.NEXTVAL) {
                    return readNextvalFunction();
                }
            }
        } else if (dataType.isCharacterType()) {
            switch (token.tokenType) {

                case Tokens.USER :
                case Tokens.CURRENT_USER :
                case Tokens.CURRENT_ROLE :
                case Tokens.SESSION_USER :
                case Tokens.SYSTEM_USER :
                case Tokens.CURRENT_CATALOG :
                case Tokens.CURRENT_SCHEMA :
                case Tokens.CURRENT_PATH :
                    FunctionSQL function =
                        FunctionSQL.newSQLFunction(token.tokenString,
                                                   compileContext);

                    e = readSQLFunction(function);
                    break;

                default :
            }
        } else if (dataType.isBooleanType()) {
            switch (token.tokenType) {

                case Tokens.TRUE :
                    read();

                    return Expression.EXPR_TRUE;

                case Tokens.FALSE :
                    read();

                    return Expression.EXPR_FALSE;
            }
        } else if (dataType.isBitType()) {
            switch (token.tokenType) {

                case Tokens.TRUE :
                    read();

                    return new ExpressionValue(BinaryData.singleBitOne,
                                               dataType);

                case Tokens.FALSE :
                    read();

                    return new ExpressionValue(BinaryData.singleBitZero,
                                               dataType);
            }
        } else if (dataType.isArrayType()) {
            e = readCollection(OpTypes.ARRAY);

            if (e.nodes.length > 0) {
                throw Error.parseError(ErrorCode.X_42562, null,
                                       scanner.getLineNumber());
            }

            e.dataType = dataType;

            return e;
        }

        if (e != null) {
            e.resolveTypes(session, null);

            if (dataType.typeComparisonGroup
                    != e.getDataType().typeComparisonGroup) {
                throw Error.parseError(ErrorCode.X_42562, null,
                                       scanner.getLineNumber());
            }

            return e;
        }

        boolean inParens = false;

        if (database.sqlSyntaxMss && token.tokenType == Tokens.OPENBRACKET) {
            read();

            inParens = true;
        }

        if (token.tokenType == Tokens.X_VALUE) {
            Object value       = token.tokenValue;
            Type   valueType   = token.dataType;
            Type   convertType = dataType;

            if (dataType.typeCode == Types.SQL_CLOB) {
                convertType = Type.getType(Types.SQL_VARCHAR, null,
                                           database.collation,
                                           dataType.precision, 0);
            } else if (dataType.typeCode == Types.SQL_BLOB) {
                convertType = Type.getType(Types.SQL_VARBINARY, null, null,
                                           dataType.precision, 0);
            }

            value = convertType.convertToType(session, value, valueType);

            read();

            if (minus) {
                value = dataType.negate(value);
            }

            if (inParens) {
                readThis(Tokens.CLOSEBRACKET);
            }

            return new ExpressionValue(value, convertType);
        } else {
            if (database.sqlSyntaxDb2) {
                Object value = null;

                switch (dataType.typeComparisonGroup) {

                    case Types.SQL_VARCHAR :
                        value = "";
                        break;

                    case Types.SQL_VARBINARY :
                        value = BinaryData.zeroLengthBinary;
                        break;

                    case Types.SQL_NUMERIC :
                        value = Integer.valueOf(0);
                        break;

                    case Types.SQL_BOOLEAN :
                        value = Boolean.FALSE;
                        break;

                    case Types.SQL_CLOB :
                        value = "";

                        return new ExpressionValue(value,
                                                   Type.SQL_VARCHAR_DEFAULT);

                    case Types.SQL_BLOB :
                        value = BinaryData.zeroLengthBinary;

                        return new ExpressionValue(value,
                                                   Type.SQL_VARBINARY_DEFAULT);

                    case Types.TIME : {
                        FunctionSQL function =
                            FunctionSQL.newSQLFunction(Tokens.T_CURRENT_TIME,
                                                       compileContext);

                        function.resolveTypes(session, null);

                        return function;
                    }
                    case Types.DATE : {
                        FunctionSQL function =
                            FunctionSQL.newSQLFunction(Tokens.T_CURRENT_DATE,
                                                       compileContext);

                        function.resolveTypes(session, null);

                        return function;
                    }
                    case Types.TIMESTAMP : {
                        FunctionSQL function = FunctionSQL.newSQLFunction(
                            Tokens.T_CURRENT_TIMESTAMP, compileContext);

                        function.resolveTypes(session, null);

                        return function;
                    }
                }

                value = dataType.convertToDefaultType(session, value);

                return new ExpressionValue(value, dataType);
            }

            throw unexpectedToken();
        }
    }

    Statement compileOpenCursorStatement(StatementCompound context) {

        readThis(Tokens.OPEN);
        checkIsSimpleName();

        String tokenString = token.tokenString;

        read();

        for (int i = 0; i < context.cursors.length; i++) {
            if (context.cursors[i].getCursorName().name.equals(tokenString)) {
                return context.cursors[i];
            }
        }

        throw Error.parseError(ErrorCode.X_34000, null,
                               scanner.getLineNumber());
    }

    Statement compileSelectSingleRowStatement(RangeGroup[] rangeGroups) {

        OrderedHashSet     variableNames = new OrderedHashSet();
        Type[]             targetTypes;
        LongDeque          colIndexList = new LongDeque();
        QuerySpecification select;

        compileContext.setOuterRanges(rangeGroups);

        select = XreadSelect();

        readThis(Tokens.INTO);

        RangeVariable[] ranges = rangeGroups[0].getRangeVariables();

        readTargetSpecificationList(variableNames, ranges, colIndexList);
        XreadTableExpression(select);
        select.setReturningResult();

        int[] columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] variables = new Expression[variableNames.size()];

        variableNames.toArray(variables);

        targetTypes = new Type[variables.length];

        for (int i = 0; i < variables.length; i++) {
            if (variables[i].getColumn().getParameterMode()
                    == SchemaObject.ParameterModes.PARAM_IN) {

                // todo - use more specific error message
                throw Error.parseError(ErrorCode.X_0U000, null,
                                       scanner.getLineNumber());
            }

            targetTypes[i] = variables[i].getDataType();
        }

        select.setReturningResult();
        select.resolve(session, rangeGroups, targetTypes);

        if (select.getColumnCount() != variables.length) {
            throw Error.error(ErrorCode.X_42564, Tokens.T_INTO);
        }

        Statement statement = new StatementSet(session, variables, select,
                                               columnMap, compileContext);

        return statement;
    }

    /**
     * Creates GET DIAGNOSTICS.
     */
    Statement compileGetStatement(RangeGroup[] rangeGroups) {

        read();
        readThis(Tokens.DIAGNOSTICS);

        OrderedHashSet  targetSet    = new OrderedHashSet();
        HsqlArrayList   exprList     = new HsqlArrayList();
        LongDeque       colIndexList = new LongDeque();
        RangeVariable[] rangeVars    = rangeGroups[0].getRangeVariables();

        readGetClauseList(rangeVars, targetSet, colIndexList, exprList);

        if (exprList.size() > 1) {
            throw Error.parseError(ErrorCode.X_42602, null,
                                   scanner.getLineNumber());
        }

        Expression expression = (Expression) exprList.get(0);

        if (expression.getDegree() != targetSet.size()) {
            throw Error.error(ErrorCode.X_42546, Tokens.T_SET);
        }

        int[] columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] targets = new Expression[targetSet.size()];

        targetSet.toArray(targets);

        for (int i = 0; i < targets.length; i++) {
            resolveOuterReferencesAndTypes(rangeGroups, targets[i]);
        }

        resolveOuterReferencesAndTypes(rangeGroups, expression);

        for (int i = 0; i < targets.length; i++) {
            if (targets[i].getColumn().getParameterMode()
                    == SchemaObject.ParameterModes.PARAM_IN) {

                // todo - use more specific error message
                throw Error.parseError(ErrorCode.X_0U000, null,
                                       scanner.getLineNumber());
            }

            if (!targets[i].getDataType().canBeAssignedFrom(
                    expression.getNodeDataType(i))) {
                throw Error.parseError(ErrorCode.X_42561, null,
                                       scanner.getLineNumber());
            }
        }

        StatementSet cs = new StatementSet(session, targets, expression,
                                           columnMap, compileContext);

        return cs;
    }

    /**
     * Creates SET Statement for PSM or session variables from this parse context.
     */
    StatementSet compileSetStatement(RangeGroup[] rangeGroups,
                                     RangeVariable[] rangeVars) {

        read();

        OrderedHashSet targetSet    = new OrderedHashSet();
        HsqlArrayList  exprList     = new HsqlArrayList();
        LongDeque      colIndexList = new LongDeque();

        readSetClauseList(rangeVars, targetSet, colIndexList, exprList);

        if (exprList.size() > 1) {
            throw Error.parseError(ErrorCode.X_42602, null,
                                   scanner.getLineNumber());
        }

        Expression expression = (Expression) exprList.get(0);

        if (expression.getDegree() != targetSet.size()) {
            throw Error.error(ErrorCode.X_42546, Tokens.T_SET);
        }

        int[] columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] targets = new Expression[targetSet.size()];

        targetSet.toArray(targets);

        for (int i = 0; i < targets.length; i++) {
            this.resolveOuterReferencesAndTypes(rangeGroups, targets[i]);
        }

        resolveOuterReferencesAndTypes(rangeGroups, expression);

        for (int i = 0; i < targets.length; i++) {
            ColumnSchema col = targets[i].getColumn();

            if (col.getParameterMode()
                    == SchemaObject.ParameterModes.PARAM_IN) {

                // todo - use more specific error message
                throw Error.error(ErrorCode.X_0U000,
                                  col.getName().statementName);
            }

            if (!targets[i].getDataType().canBeAssignedFrom(
                    expression.getNodeDataType(i))) {
                throw Error.parseError(ErrorCode.X_42561, null,
                                       scanner.getLineNumber());
            }
        }

        StatementSet cs = new StatementSet(session, targets, expression,
                                           columnMap, compileContext);

        return cs;
    }

    /**
     * Creates SET Statement for a trigger row from this parse context.
     */
    StatementDMQL compileTriggerSetStatement(Table table,
            RangeGroup[] rangeGroups) {

        read();

        Expression[]   updateExpressions;
        int[]          columnMap;
        OrderedHashSet targetSet = new OrderedHashSet();
        HsqlArrayList  exprList  = new HsqlArrayList();
        RangeVariable[] targetRangeVars = new RangeVariable[]{
            rangeGroups[0].getRangeVariables()[TriggerDef.NEW_ROW] };
        LongDeque colIndexList = new LongDeque();

        readSetClauseList(targetRangeVars, targetSet, colIndexList, exprList);

        columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] targets = new Expression[targetSet.size()];

        targetSet.toArray(targets);

        for (int i = 0; i < targets.length; i++) {
            resolveOuterReferencesAndTypes(RangeGroup.emptyArray, targets[i]);
        }

        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);
        resolveUpdateExpressions(table, RangeGroup.emptyGroup, columnMap,
                                 updateExpressions, rangeGroups);

        StatementDMQL cs = new StatementSet(session, targets, table,
                                            rangeGroups[0].getRangeVariables(),
                                            columnMap, updateExpressions,
                                            compileContext);

        return cs;
    }

    StatementSchema compileAlterSpecificRoutine() {

        boolean restrict = false;

        readThis(Tokens.SPECIFIC);
        readThis(Tokens.ROUTINE);

        Routine routine =
            (Routine) readSchemaObjectName(SchemaObject.SPECIFIC_ROUTINE);

        routine = routine.duplicate();

        readRoutineCharacteristics(routine);

        restrict = readIfThis(Tokens.RESTRICT);

        if (restrict) {
            OrderedHashSet set = database.schemaManager.getReferencesTo(
                routine.getSpecificName());

            if (!set.isEmpty()) {
                throw Error.parseError(ErrorCode.X_42502, null,
                                       scanner.getLineNumber());
            }
        }

        if (token.tokenType == Tokens.BODY) {
            read();
        } else if (token.tokenType == Tokens.NAME) {
            read();
        }

        readRoutineBody(routine);
        routine.resetAlteredRoutineSettings();
        routine.resolve(session);

        Object[] args = new Object[]{ routine };
        String   sql  = getLastPart();
        StatementSchema cs = new StatementSchema(sql,
            StatementTypes.ALTER_ROUTINE, args, null,
            database.schemaManager.getCatalogNameArray());

        return cs;
    }

    // SQL-invoked routine
    StatementSchema compileCreateProcedureOrFunction(boolean orReplace) {

        int     routineType;
        boolean isAggregate = false;

        if (token.tokenType == Tokens.AGGREGATE) {
            isAggregate = true;

            read();

            if (token.tokenType == Tokens.PROCEDURE) {
                throw unexpectedToken();
            }
        }

        routineType = token.tokenType == Tokens.PROCEDURE
                      ? SchemaObject.PROCEDURE
                      : SchemaObject.FUNCTION;

        HsqlName name;

        read();

        name = readNewSchemaObjectName(routineType, true);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Routine routine = new Routine(routineType);

        routine.setName(name);
        routine.setAggregate(isAggregate);
        readThis(Tokens.OPENBRACKET);

        if (token.tokenType == Tokens.CLOSEBRACKET) {
            read();
        } else {
            while (true) {
                ColumnSchema newcolumn = readRoutineParameter(routine, true);

                routine.addParameter(newcolumn);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    readThis(Tokens.CLOSEBRACKET);

                    break;
                }
            }
        }

        if (routineType != SchemaObject.PROCEDURE) {
            readThis(Tokens.RETURNS);

            if (token.tokenType == Tokens.TABLE) {
                read();

                TableDerived table =
                    new TableDerived(database, SqlInvariants.MODULE_HSQLNAME,
                                     TableBase.FUNCTION_TABLE);

                readTableDefinition(routine, table);
                routine.setReturnTable(table);
            } else {
                Type type = readTypeDefinition(false, true);

                routine.setReturnType(type);
            }
        }

        readRoutineCharacteristics(routine);
        readRoutineBody(routine);

        Object[] args = new Object[]{ routine };
        String   sql  = getLastPart();
        StatementSchema cs = new StatementSchema(sql,
            StatementTypes.CREATE_ROUTINE, args, null,
            database.schemaManager.getCatalogNameArray());

        return cs;
    }

    Routine readCreatePasswordCheckFunction() {

        Routine routine = new Routine(SchemaObject.FUNCTION);

        if (token.tokenType == Tokens.NONE) {
            read();

            return null;
        } else if (token.tokenType == Tokens.EXTERNAL) {
            routine.setLanguage(Routine.LANGUAGE_JAVA);
            routine.setDataImpact(Routine.NO_SQL);
        } else {
            routine.setLanguage(Routine.LANGUAGE_SQL);
            routine.setDataImpact(Routine.CONTAINS_SQL);
        }

        HsqlName hsqlName = database.nameManager.newHsqlName(Tokens.T_PASSWORD,
            false, SchemaObject.FUNCTION);

        hsqlName.setSchemaIfNull(SqlInvariants.SYSTEM_SCHEMA_HSQLNAME);
        routine.setName(hsqlName);

        hsqlName = database.nameManager.newHsqlName(Tokens.T_PASSWORD, false,
                SchemaObject.PARAMETER);

        ColumnSchema column = new ColumnSchema(hsqlName, Type.SQL_VARCHAR,
                                               false, false, null);

        routine.addParameter(column);
        routine.setReturnType(Type.SQL_BOOLEAN);
        readRoutineBody(routine);
        routine.resolve(session);

        return routine;
    }

    Routine readCreateDatabaseAuthenticationFunction() {

        Routine routine = new Routine(SchemaObject.FUNCTION);

        if (token.tokenType == Tokens.NONE) {
            read();

            return null;
        }

        checkIsThis(Tokens.EXTERNAL);
        routine.setLanguage(Routine.LANGUAGE_JAVA);
        routine.setDataImpact(Routine.NO_SQL);
        routine.setName(
            database.nameManager.newHsqlName(
                Tokens.T_AUTHENTICATION, false, SchemaObject.FUNCTION));

        for (int i = 0; i < 3; i++) {
            ColumnSchema column = new ColumnSchema(null, Type.SQL_VARCHAR,
                                                   false, false, null);

            routine.addParameter(column);
        }

        routine.setReturnType(
            new ArrayType(
                Type.SQL_VARCHAR_DEFAULT, ArrayType.defaultArrayCardinality));
        readRoutineBody(routine);
        routine.resolve(session);

        return routine;
    }

    private void readTableDefinition(Routine routine,
                                     Table table) throws HsqlException {

        readThis(Tokens.OPENBRACKET);

        for (int i = 0; ; i++) {
            ColumnSchema newcolumn = readRoutineParameter(routine, false);

            if (newcolumn.getName() == null) {
                throw unexpectedToken();
            }

            table.addColumn(newcolumn);

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                readThis(Tokens.CLOSEBRACKET);

                break;
            }
        }

        table.createPrimaryKey();
    }

    private void readRoutineCharacteristics(Routine routine) {

        OrderedIntHashSet set = new OrderedIntHashSet();
        boolean           end = false;

        while (!end) {
            switch (token.tokenType) {

                case Tokens.LANGUAGE : {
                    if (!set.add(Tokens.LANGUAGE)) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.JAVA) {
                        read();
                        routine.setLanguage(Routine.LANGUAGE_JAVA);
                    } else if (token.tokenType == Tokens.SQL) {
                        read();
                        routine.setLanguage(Routine.LANGUAGE_SQL);
                    } else {
                        throw unexpectedToken();
                    }

                    break;
                }
                case Tokens.PARAMETER : {
                    if (!set.add(Tokens.PARAMETER)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.STYLE);

                    if (token.tokenType == Tokens.JAVA) {
                        read();
                        routine.setParameterStyle(Routine.PARAM_STYLE_JAVA);
                    } else {
                        readThis(Tokens.SQL);
                        routine.setParameterStyle(Routine.PARAM_STYLE_SQL);
                    }

                    break;
                }
                case Tokens.SPECIFIC : {
                    if (!set.add(Tokens.SPECIFIC)) {
                        throw unexpectedToken();
                    }

                    read();

                    HsqlName name =
                        readNewSchemaObjectName(SchemaObject.SPECIFIC_ROUTINE,
                                                false);

                    routine.setSpecificName(name);

                    break;
                }
                case Tokens.DETERMINISTIC : {
                    if (!set.add(Tokens.DETERMINISTIC)) {
                        throw unexpectedToken();
                    }

                    read();
                    routine.setDeterministic(true);

                    break;
                }
                case Tokens.NOT : {
                    if (!set.add(Tokens.DETERMINISTIC)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.DETERMINISTIC);
                    routine.setDeterministic(false);

                    break;
                }
                case Tokens.MODIFIES : {
                    if (!set.add(Tokens.SQL)) {
                        throw unexpectedToken();
                    }

                    if (routine.getType() == SchemaObject.FUNCTION) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SQL);
                    readThis(Tokens.DATA);
                    routine.setDataImpact(Routine.MODIFIES_SQL);

                    break;
                }
                case Tokens.NO : {
                    if (!set.add(Tokens.SQL)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SQL);
                    routine.setDataImpact(Routine.NO_SQL);

                    break;
                }
                case Tokens.READS : {
                    if (!set.add(Tokens.SQL)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SQL);
                    readThis(Tokens.DATA);
                    routine.setDataImpact(Routine.READS_SQL);

                    break;
                }
                case Tokens.CONTAINS : {
                    if (!set.add(Tokens.SQL)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SQL);
                    routine.setDataImpact(Routine.CONTAINS_SQL);

                    break;
                }
                case Tokens.RETURNS : {
                    if (!set.add(Tokens.NULL) || routine.isProcedure()) {
                        throw unexpectedToken();
                    }

                    if (routine.isAggregate()) {
                        throw Error.error(ErrorCode.X_42604,
                                          token.tokenString);
                    }

                    read();
                    readThis(Tokens.NULL);
                    readThis(Tokens.ON);
                    readThis(Tokens.NULL);
                    readThis(Tokens.INPUT);
                    routine.setNullInputOutput(true);

                    break;
                }
                case Tokens.CALLED : {
                    if (!set.add(Tokens.NULL) || routine.isProcedure()) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.ON);
                    readThis(Tokens.NULL);
                    readThis(Tokens.INPUT);
                    routine.setNullInputOutput(false);

                    break;
                }
                case Tokens.DYNAMIC : {
                    if (!set.add(Tokens.RESULT) || routine.isFunction()) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.RESULT);
                    readThis(Tokens.SETS);

                    int results = readInteger();

                    if (results < 0 || results > 16) {
                        throw Error.error(ErrorCode.X_42604,
                                          String.valueOf(results));
                    }

                    routine.setMaxDynamicResults(results);

                    break;
                }
                case Tokens.NEW : {
                    if (routine.getType() == SchemaObject.FUNCTION
                            || !set.add(Tokens.SAVEPOINT)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SAVEPOINT);
                    readThis(Tokens.LEVEL);
                    routine.setNewSavepointLevel(true);

                    break;
                }
                case Tokens.OLD : {
                    if (routine.getType() == SchemaObject.FUNCTION
                            || !set.add(Tokens.SAVEPOINT)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.SAVEPOINT);
                    readThis(Tokens.LEVEL);
                    routine.setNewSavepointLevel(false);

                    throw super.unsupportedFeature(Tokens.T_OLD);

                    // break;
                }
                default :
                    end = true;
                    break;
            }
        }
    }

    void readRoutineBody(Routine routine) {

        if (token.tokenType == Tokens.EXTERNAL) {
            if (routine.getLanguage() != Routine.LANGUAGE_JAVA) {
                throw unexpectedToken();
            }

            read();
            readThis(Tokens.NAME);
            checkIsValue(Types.SQL_CHAR);
            routine.setMethodURL((String) token.tokenValue);
            read();

            if (token.tokenType == Tokens.PARAMETER) {
                read();
                readThis(Tokens.STYLE);
                readThis(Tokens.JAVA);
            }
        } else {
            startRecording();

            Statement statement = compileSQLProcedureStatementOrNull(routine,
                null);

            if (statement == null) {
                throw unexpectedToken();
            }

            Token[] tokenisedStatement = getRecordedStatement();
            String  sql                = Token.getSQL(tokenisedStatement);

            statement.setSQL(sql);
            routine.setProcedure(statement);
        }
    }

    /*
        <SQL control statement> ::=
        <call statement>
        | <return statement>

        <compound statement>
        <case statement>
        <if statement>
        <iterate statement>
        <leave statement>
        <loop statement>
        <while statement>
        <repeat statement>
       <for statement>
       <assignment statement> SET (,,,) = (,,,) or SET a = b


     */
    private Object[] readLocalDeclarationList(Routine routine,
            StatementCompound context) {

        HsqlArrayList list                = new HsqlArrayList();
        final int     table               = 0;
        final int     variableOrCondition = 1;
        final int     cursor              = 2;
        final int     handler             = 3;
        int           objectType          = table;
        RangeGroup[]  rangeGroups         = new RangeGroup[1];

        rangeGroups[0] = context == null ? routine
                                         : context;

        compileContext.setOuterRanges(rangeGroups);

        while (token.tokenType == Tokens.DECLARE) {
            Object var = null;

            if (objectType == table) {
                var = readLocalTableVariableDeclarationOrNull(routine);

                if (var == null) {
                    objectType = variableOrCondition;
                } else {
                    list.add(var);
                    readThis(Tokens.SEMICOLON);
                }
            } else if (objectType == variableOrCondition) {
                var = readLocalVariableDeclarationOrNull();

                if (var == null) {
                    objectType = cursor;
                } else {
                    list.addAll((Object[]) var);
                }
            } else if (objectType == cursor) {
                var = compileDeclareCursor(rangeGroups, true);

                if (var == null) {
                    objectType = handler;
                } else {
                    list.add(var);
                    readThis(Tokens.SEMICOLON);
                }
            } else if (objectType == handler) {
                var = compileLocalHandlerDeclaration(routine, context);

                list.add(var);
            }
        }

        Object[] declarations = new Object[list.size()];

        list.toArray(declarations);

        return declarations;
    }

    Table readLocalTableVariableDeclarationOrNull(Routine routine) {

        int position = super.getPosition();

        readThis(Tokens.DECLARE);

        if (token.tokenType == Tokens.TABLE) {
            read();

            HsqlName name = super.readNewSchemaObjectName(SchemaObject.TABLE,
                false);

            name.schema = SqlInvariants.MODULE_HSQLNAME;

            Table table = new Table(database, name, TableBase.TEMP_TABLE);

            table.persistenceScope = TableBase.SCOPE_ROUTINE;

            readTableDefinition(routine, table);

            return table;
        } else {
            rewind(position);

            return null;
        }
    }

    ColumnSchema[] readLocalVariableDeclarationOrNull() {

        int        position = super.getPosition();
        Type       type;
        HsqlName[] names = HsqlName.emptyArray;

        try {
            readThis(Tokens.DECLARE);

            if (isReservedKey()) {
                rewind(position);

                return null;
            }

            while (true) {
                names = (HsqlName[]) ArrayUtil.resizeArray(names,
                        names.length + 1);
                names[names.length - 1] =
                    super.readNewSchemaObjectName(SchemaObject.VARIABLE,
                                                  false);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    break;
                }
            }

            type = readTypeDefinition(false, true);
        } catch (HsqlException e) {

            // may be cursor
            rewind(position);

            return null;
        }

        Expression def = null;

        if (token.tokenType == Tokens.DEFAULT) {
            read();

            def = readDefaultClause(type);
        }

        ColumnSchema[] variable = new ColumnSchema[names.length];

        for (int i = 0; i < names.length; i++) {
            variable[i] = new ColumnSchema(names[i], type, true, false, def);

            variable[i].setParameterMode(
                SchemaObject.ParameterModes.PARAM_INOUT);
        }

        readThis(Tokens.SEMICOLON);

        return variable;
    }

    private StatementHandler compileLocalHandlerDeclaration(Routine routine,
            StatementCompound context) {

        int handlerType;

        readThis(Tokens.DECLARE);

        switch (token.tokenType) {

            case Tokens.CONTINUE :
                read();

                handlerType = StatementHandler.CONTINUE;
                break;

            case Tokens.EXIT :
                read();

                handlerType = StatementHandler.EXIT;
                break;

            case Tokens.UNDO :
                read();

                handlerType = StatementHandler.UNDO;
                break;

            default :
                throw unexpectedToken();
        }

        readThis(Tokens.HANDLER);
        readThis(Tokens.FOR);

        StatementHandler handler = new StatementHandler(handlerType);
        boolean          end     = false;
        boolean          start   = true;

        while (!end) {
            int conditionType = StatementHandler.NONE;

            switch (token.tokenType) {

                case Tokens.COMMA :
                    if (start) {
                        throw unexpectedToken();
                    }

                    read();

                    start = true;
                    break;

                case Tokens.SQLSTATE :
                    conditionType = StatementHandler.SQL_STATE;

                // fall through
                case Tokens.SQLEXCEPTION :
                    if (conditionType == StatementHandler.NONE) {
                        conditionType = StatementHandler.SQL_EXCEPTION;
                    }

                // fall through
                case Tokens.SQLWARNING :
                    if (conditionType == StatementHandler.NONE) {
                        conditionType = StatementHandler.SQL_WARNING;
                    }

                // fall through
                case Tokens.NOT :
                    if (conditionType == StatementHandler.NONE) {
                        conditionType = StatementHandler.SQL_NOT_FOUND;
                    }

                    if (!start) {
                        throw unexpectedToken();
                    }

                    start = false;

                    read();

                    if (conditionType == StatementHandler.SQL_NOT_FOUND) {
                        readThis(Tokens.FOUND);
                    } else if (conditionType == StatementHandler.SQL_STATE) {
                        String sqlState = parseSQLStateValue();

                        handler.addConditionState(sqlState);

                        break;
                    }

                    handler.addConditionType(conditionType);
                    break;

                default :
                    if (start) {
                        throw unexpectedToken();
                    }

                    end = true;
                    break;
            }
        }

        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        } else {
            Statement e = compileSQLProcedureStatementOrNull(routine, context);

            if (e == null) {
                throw unexpectedToken();
            }

            readThis(Tokens.SEMICOLON);
            handler.addStatement(e);
        }

        return handler;
    }

    String parseSQLStateValue() {

        readIfThis(Tokens.VALUE);
        checkIsValue(Types.SQL_CHAR);

        String sqlState = token.tokenString;

        if (token.tokenString.length() != 5) {
            throw Error.parseError(ErrorCode.X_42607, null,
                                   scanner.getLineNumber());
        }

        read();

        return sqlState;
    }

    private Statement compileCompoundStatement(Routine routine,
            StatementCompound context, HsqlName label) {

        final boolean atomic = true;

        readThis(Tokens.BEGIN);
        readThis(Tokens.ATOMIC);

        if (label == null) {
            String            labelString;
            StatementCompound parent = context;
            int               level  = 0;

            while (parent != null) {
                level++;

                parent = parent.parent;
            }

            labelString = "_" + level;
            label = session.database.nameManager.newHsqlName(labelString,
                    false, SchemaObject.LABEL);
        }

        StatementCompound statement =
            new StatementCompound(StatementTypes.BEGIN_END, label);

        statement.setAtomic(atomic);
        statement.setRoot(routine);
        statement.setParent(context);

        Object[] declarations = readLocalDeclarationList(routine, context);

        statement.setLocalDeclarations(declarations);
        session.sessionContext.pushRoutineTables(statement.scopeTables);

        try {
            Statement[] statements = compileSQLProcedureStatementList(routine,
                statement);

            statement.setStatements(statements);
        } finally {
            session.sessionContext.popRoutineTables();
        }

        readThis(Tokens.END);

        if (isSimpleName() && !isReservedKey()) {
            if (label == null) {
                throw unexpectedToken();
            }

            if (!label.name.equals(token.tokenString)) {
                throw Error.error(ErrorCode.X_42508, token.tokenString);
            }

            read();
        }

        return statement;
    }

    private Statement[] compileSQLProcedureStatementList(Routine routine,
            StatementCompound context) {

        Statement     e;
        HsqlArrayList list = new HsqlArrayList();

        while (true) {
            e = compileSQLProcedureStatementOrNull(routine, context);

            if (e == null) {
                break;
            }

            readThis(Tokens.SEMICOLON);
            list.add(e);
        }

        if (list.size() == 0) {
            throw unexpectedToken();
        }

        Statement[] statements = new Statement[list.size()];

        list.toArray(statements);

        return statements;
    }

    Statement compileSQLProcedureStatementOrNull(Routine routine,
            StatementCompound context) {

        Statement    cs          = null;
        HsqlName     label       = null;
        RangeGroup   rangeGroup  = context == null ? routine
                                                   : context;
        RangeGroup[] rangeGroups = new RangeGroup[]{ rangeGroup };

        if (!routine.isTrigger() && isSimpleName() && !isReservedKey()) {
            label = readNewSchemaObjectName(SchemaObject.LABEL, false);

            // todo - improved error message
            if (token.tokenType != Tokens.COLON) {
                throw unexpectedToken(label.getNameString());
            }

            readThis(Tokens.COLON);
        }

        compileContext.reset();

        HsqlName oldSchema = session.getCurrentSchemaHsqlName();

        session.setCurrentSchemaHsqlName(routine.getSchemaName());

        try {
            switch (token.tokenType) {

                // data
                case Tokens.OPEN : {
                    if (routine.dataImpact == Routine.CONTAINS_SQL) {
                        throw Error.error(ErrorCode.X_42602,
                                          routine.getDataImpactString());
                    }

                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileOpenCursorStatement(context);

                    break;
                }
                case Tokens.SELECT : {
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileSelectSingleRowStatement(rangeGroups);

                    break;
                }

                // data change
                case Tokens.INSERT :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileInsertStatement(rangeGroups);
                    break;

                case Tokens.UPDATE :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileUpdateStatement(rangeGroups);
                    break;

                case Tokens.DELETE :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileDeleteStatement(rangeGroups);
                    break;

                case Tokens.TRUNCATE :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileTruncateStatement();
                    break;

                case Tokens.MERGE :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileMergeStatement(rangeGroups);
                    break;

                case Tokens.SET :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    if (routine.isTrigger()) {
                        if (routine.triggerType == TriggerDef.BEFORE
                                && routine.triggerOperation
                                   != StatementTypes.DELETE_WHERE) {
                            int position = super.getPosition();

                            try {
                                cs = compileTriggerSetStatement(
                                    routine.triggerTable, rangeGroups);

                                break;
                            } catch (HsqlException e) {
                                rewind(position);

                                cs = compileSetStatement(
                                    rangeGroups,
                                    rangeGroup.getRangeVariables());
                            }
                        } else {
                            cs = compileSetStatement(
                                rangeGroups, rangeGroup.getRangeVariables());
                        }

                        ((StatementSet) cs).checkIsNotColumnTarget();
                    } else {
                        cs = compileSetStatement(
                            rangeGroups, rangeGroup.getRangeVariables());
                    }
                    break;

                case Tokens.GET :
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = this.compileGetStatement(rangeGroups);
                    break;

                // control
                case Tokens.CALL : {
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileCallStatement(rangeGroups, true);

                    Routine proc = ((StatementProcedure) cs).procedure;

                    if (proc != null) {
                        switch (routine.dataImpact) {

                            case Routine.CONTAINS_SQL : {
                                if (proc.dataImpact == Routine.READS_SQL
                                        || proc.dataImpact
                                           == Routine.MODIFIES_SQL) {
                                    throw Error.error(
                                        ErrorCode.X_42602,
                                        routine.getDataImpactString());
                                }

                                break;
                            }
                            case Routine.READS_SQL : {
                                if (proc.dataImpact == Routine.MODIFIES_SQL) {
                                    throw Error.error(
                                        ErrorCode.X_42602,
                                        routine.getDataImpactString());
                                }

                                break;
                            }
                        }
                    }

                    break;
                }
                case Tokens.RETURN : {
                    if (routine.isTrigger() || label != null) {
                        throw unexpectedToken();
                    }

                    read();

                    cs = compileReturnValue(routine, context);

                    break;
                }
                case Tokens.BEGIN : {
                    cs = compileCompoundStatement(routine, context, label);

                    break;
                }
                case Tokens.WHILE : {
                    if (routine.isTrigger()) {
                        throw unexpectedToken();
                    }

                    cs = compileWhile(routine, context, label);

                    break;
                }
                case Tokens.REPEAT : {
                    cs = compileRepeat(routine, context, label);

                    break;
                }
                case Tokens.LOOP : {
                    cs = compileLoop(routine, context, label);

                    break;
                }
                case Tokens.FOR : {
                    cs = compileFor(routine, context, label);

                    break;
                }
                case Tokens.ITERATE : {
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileIterate();

                    break;
                }
                case Tokens.LEAVE : {
                    if (label != null) {
                        throw unexpectedToken();
                    }

                    cs = compileLeave(routine, context);

                    break;
                }
                case Tokens.IF : {
                    cs = compileIf(routine, context);

                    break;
                }
                case Tokens.CASE : {
                    cs = compileCase(routine, context);

                    break;
                }
                case Tokens.SIGNAL : {
                    cs = compileSignal(routine, context, label);

                    break;
                }
                case Tokens.RESIGNAL : {
                    cs = compileResignal(routine, context, label);

                    break;
                }
                default :
                    return null;
            }

            cs.setRoot(routine);
            cs.setParent(context);

            return cs;
        } finally {
            session.setCurrentSchemaHsqlName(oldSchema);
        }
    }

    private Statement compileReturnValue(Routine routine,
                                         StatementCompound context) {

        RangeGroup[] rangeGroups = new RangeGroup[1];

        rangeGroups[0] = context == null ? routine
                                         : context;

        compileContext.setOuterRanges(rangeGroups);

        Expression e = XreadValueExpressionOrNull();

        if (e == null) {
            throw unexpectedToken();
        }

        resolveOuterReferencesAndTypes(routine, context, e);

        if (routine.isProcedure()) {
            throw Error.parseError(ErrorCode.X_42602, null,
                                   scanner.getLineNumber());
        }

        return new StatementExpression(session, compileContext,
                                       StatementTypes.RETURN, e);
    }

    private Statement compileIterate() {

        readThis(Tokens.ITERATE);

        HsqlName label = readNewSchemaObjectName(SchemaObject.LABEL, false);

        return new StatementSimple(StatementTypes.ITERATE, label);
    }

    private Statement compileLeave(Routine routine,
                                   StatementCompound context) {

        readThis(Tokens.LEAVE);

        HsqlName label = readNewSchemaObjectName(SchemaObject.LABEL, false);

        return new StatementSimple(StatementTypes.LEAVE, label);
    }

    private Statement compileWhile(Routine routine, StatementCompound context,
                                   HsqlName label) {

        readThis(Tokens.WHILE);

        Expression e = XreadBooleanValueExpression();

        resolveOuterReferencesAndTypes(routine, context, e);

        StatementExpression condition = new StatementExpression(session,
            compileContext, StatementTypes.CONDITION, e);

        readThis(Tokens.DO);

        Statement[] statements = compileSQLProcedureStatementList(routine,
            context);

        readThis(Tokens.END);
        readThis(Tokens.WHILE);

        if (isSimpleName() && !isReservedKey()) {
            if (label == null) {
                throw unexpectedToken();
            }

            if (!label.name.equals(token.tokenString)) {
                throw Error.error(ErrorCode.X_42508, token.tokenString);
            }

            read();
        }

        StatementCompound statement =
            new StatementCompound(StatementTypes.WHILE, label);

        statement.setStatements(statements);
        statement.setCondition(condition);

        return statement;
    }

    private Statement compileRepeat(Routine routine,
                                    StatementCompound context,
                                    HsqlName label) {

        readThis(Tokens.REPEAT);

        Statement[] statements = compileSQLProcedureStatementList(routine,
            context);

        readThis(Tokens.UNTIL);

        Expression e = XreadBooleanValueExpression();

        resolveOuterReferencesAndTypes(routine, context, e);

        StatementExpression condition = new StatementExpression(session,
            compileContext, StatementTypes.CONDITION, e);

        readThis(Tokens.END);
        readThis(Tokens.REPEAT);

        if (isSimpleName() && !isReservedKey()) {
            if (label == null) {
                throw unexpectedToken();
            }

            if (!label.name.equals(token.tokenString)) {
                throw Error.error(ErrorCode.X_42508, token.tokenString);
            }

            read();
        }

        StatementCompound statement =
            new StatementCompound(StatementTypes.REPEAT, label);

        statement.setStatements(statements);
        statement.setCondition(condition);

        return statement;
    }

    private Statement compileLoop(Routine routine, StatementCompound context,
                                  HsqlName label) {

        readThis(Tokens.LOOP);

        Statement[] statements = compileSQLProcedureStatementList(routine,
            context);

        readThis(Tokens.END);
        readThis(Tokens.LOOP);

        if (isSimpleName() && !isReservedKey()) {
            if (label == null) {
                throw unexpectedToken();
            }

            if (!label.name.equals(token.tokenString)) {
                throw Error.error(ErrorCode.X_42508, token.tokenString);
            }

            read();
        }

        StatementCompound result = new StatementCompound(StatementTypes.LOOP,
            label);

        result.setStatements(statements);

        return result;
    }

    private Statement compileFor(Routine routine, StatementCompound context,
                                 HsqlName label) {

        RangeGroup[] rangeGroups = new RangeGroup[1];

        rangeGroups[0] = context == null ? routine
                                         : context;

        compileContext.setOuterRanges(rangeGroups);
        readThis(Tokens.FOR);

        StatementQuery cursorStatement =
            compileCursorSpecification(rangeGroups,
                                       ResultProperties.defaultPropsValue,
                                       false);

        readThis(Tokens.DO);

        StatementCompound forStatement =
            new StatementCompound(StatementTypes.FOR, label);

        forStatement.setAtomic(true);
        forStatement.setRoot(routine);
        forStatement.setParent(context);
        forStatement.setLoopStatement(cursorStatement);

        Statement[] statements = compileSQLProcedureStatementList(routine,
            forStatement);

        readThis(Tokens.END);
        readThis(Tokens.FOR);

        if (isSimpleName() && !isReservedKey()) {
            if (label == null) {
                throw unexpectedToken();
            }

            if (!label.name.equals(token.tokenString)) {
                throw Error.error(ErrorCode.X_42508, token.tokenString);
            }

            read();
        }

        forStatement.setStatements(statements);

        return forStatement;
    }

    private Statement compileIf(Routine routine, StatementCompound context) {

        HsqlArrayList list = new HsqlArrayList();

        readThis(Tokens.IF);

        Expression e = XreadBooleanValueExpression();

        resolveOuterReferencesAndTypes(routine, context, e);

        Statement statement = new StatementExpression(session, compileContext,
            StatementTypes.CONDITION, e);

        list.add(statement);
        readThis(Tokens.THEN);

        Statement[] statements = compileSQLProcedureStatementList(routine,
            context);

        for (int i = 0; i < statements.length; i++) {
            list.add(statements[i]);
        }

        while (token.tokenType == Tokens.ELSEIF) {
            read();

            e = XreadBooleanValueExpression();

            resolveOuterReferencesAndTypes(routine, context, e);

            statement = new StatementExpression(session, compileContext,
                                                StatementTypes.CONDITION, e);

            list.add(statement);
            readThis(Tokens.THEN);

            statements = compileSQLProcedureStatementList(routine, context);

            for (int i = 0; i < statements.length; i++) {
                list.add(statements[i]);
            }
        }

        if (token.tokenType == Tokens.ELSE) {
            read();

            e = Expression.EXPR_TRUE;
            statement = new StatementExpression(session, compileContext,
                                                StatementTypes.CONDITION, e);

            list.add(statement);

            statements = compileSQLProcedureStatementList(routine, context);

            for (int i = 0; i < statements.length; i++) {
                list.add(statements[i]);
            }
        }

        readThis(Tokens.END);
        readThis(Tokens.IF);

        statements = new Statement[list.size()];

        list.toArray(statements);

        StatementCompound result = new StatementCompound(StatementTypes.IF,
            null);

        result.setStatements(statements);

        return result;
    }

    private Statement compileCase(Routine routine, StatementCompound context) {

        HsqlArrayList list      = new HsqlArrayList();
        Expression    condition = null;
        Statement     statement;
        Statement[]   statements;

        readThis(Tokens.CASE);

        if (token.tokenType == Tokens.WHEN) {
            list = readCaseWhen(routine, context);
        } else {
            list = readSimpleCaseWhen(routine, context);
        }

        if (token.tokenType == Tokens.ELSE) {
            read();

            condition = Expression.EXPR_TRUE;
            statement = new StatementExpression(session, compileContext,
                                                StatementTypes.CONDITION,
                                                condition);

            list.add(statement);

            statements = compileSQLProcedureStatementList(routine, context);

            for (int i = 0; i < statements.length; i++) {
                list.add(statements[i]);
            }
        }

        readThis(Tokens.END);
        readThis(Tokens.CASE);

        statements = new Statement[list.size()];

        list.toArray(statements);

        StatementCompound result = new StatementCompound(StatementTypes.IF,
            null);

        result.setStatements(statements);

        return result;
    }

    private HsqlArrayList readSimpleCaseWhen(Routine routine,
            StatementCompound context) {

        HsqlArrayList list      = new HsqlArrayList();
        Expression    condition = null;
        Statement     statement;
        Statement[]   statements;
        Expression    predicand = XreadRowValuePredicand();

        do {
            readThis(Tokens.WHEN);

            do {
                Expression newCondition = XreadPredicateRightPart(predicand);

                if (predicand == newCondition) {
                    newCondition =
                        new ExpressionLogical(predicand,
                                              XreadRowValuePredicand());
                }

                resolveOuterReferencesAndTypes(routine, context, newCondition);

                if (condition == null) {
                    condition = newCondition;
                } else {
                    condition = new ExpressionLogical(OpTypes.OR, condition,
                                                      newCondition);
                }

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    break;
                }
            } while (true);

            statement = new StatementExpression(session, compileContext,
                                                StatementTypes.CONDITION,
                                                condition);

            list.add(statement);
            readThis(Tokens.THEN);

            statements = compileSQLProcedureStatementList(routine, context);

            for (int i = 0; i < statements.length; i++) {
                list.add(statements[i]);
            }

            if (token.tokenType != Tokens.WHEN) {
                break;
            }
        } while (true);

        return list;
    }

    private HsqlArrayList readCaseWhen(Routine routine,
                                       StatementCompound context) {

        HsqlArrayList list      = new HsqlArrayList();
        Expression    condition = null;
        Statement     statement;
        Statement[]   statements;

        do {
            readThis(Tokens.WHEN);

            condition = XreadBooleanValueExpression();

            resolveOuterReferencesAndTypes(routine, context, condition);

            statement = new StatementExpression(session, compileContext,
                                                StatementTypes.CONDITION,
                                                condition);

            list.add(statement);
            readThis(Tokens.THEN);

            statements = compileSQLProcedureStatementList(routine, context);

            for (int i = 0; i < statements.length; i++) {
                list.add(statements[i]);
            }

            if (token.tokenType != Tokens.WHEN) {
                break;
            }
        } while (true);

        return list;
    }

    private Statement compileSignal(Routine routine,
                                    StatementCompound context,
                                    HsqlName label) {

        String     sqlState;
        Expression message = null;

        readThis(Tokens.SIGNAL);
        readThis(Tokens.SQLSTATE);

        sqlState = parseSQLStateValue();

        if (readIfThis(Tokens.SET)) {
            readThis(Tokens.MESSAGE_TEXT);
            readThis(Tokens.EQUALS);

            message = XreadSimpleValueSpecificationOrNull();

            if (message == null) {
                throw unexpectedToken();
            }

            resolveOuterReferencesAndTypes(routine, context, message);
        }

        StatementSimple cs = new StatementSimple(StatementTypes.SIGNAL,
            sqlState, message);

        return cs;
    }

    private Statement compileResignal(Routine routine,
                                      StatementCompound context,
                                      HsqlName label) {

        String     sqlState = null;
        Expression message  = null;

        readThis(Tokens.RESIGNAL);

        if (readIfThis(Tokens.SQLSTATE)) {
            sqlState = parseSQLStateValue();

            if (readIfThis(Tokens.SET)) {
                readThis(Tokens.MESSAGE_TEXT);
                readThis(Tokens.EQUALS);

                message = XreadSimpleValueSpecificationOrNull();

                if (message == null) {
                    throw unexpectedToken();
                }

                resolveOuterReferencesAndTypes(routine, context, message);
            }
        }

        StatementSimple cs = new StatementSimple(StatementTypes.RESIGNAL,
            sqlState, message);

        return cs;
    }

    private ColumnSchema readRoutineParameter(Routine routine,
            boolean isParam) {

        HsqlName hsqlName      = null;
        byte     parameterMode = SchemaObject.ParameterModes.PARAM_IN;

        if (isParam) {
            switch (token.tokenType) {

                case Tokens.IN :
                    read();
                    break;

                case Tokens.OUT :
                    if (routine.getType() != SchemaObject.PROCEDURE) {
                        throw unexpectedToken();
                    }

                    read();

                    parameterMode = SchemaObject.ParameterModes.PARAM_OUT;
                    break;

                case Tokens.INOUT :
                    if (routine.getType() != SchemaObject.PROCEDURE) {
                        if (!routine.isAggregate()) {
                            throw unexpectedToken();
                        }
                    }

                    read();

                    parameterMode = SchemaObject.ParameterModes.PARAM_INOUT;
                    break;

                default :
            }
        }

        if (!isReservedKey()) {
            hsqlName = readNewDependentSchemaObjectName(routine.getName(),
                    SchemaObject.PARAMETER);
        }

        Type typeObject = readTypeDefinition(false, true);
        ColumnSchema column = new ColumnSchema(hsqlName, typeObject, true,
                                               false, null);

        if (isParam) {
            column.setParameterMode(parameterMode);
        }

        return column;
    }

    void resolveOuterReferencesAndTypes(Routine routine,
                                        StatementCompound context,
                                        Expression e) {

        RangeGroup rangeGroup = context == null ? routine
                                                : context;

        resolveOuterReferencesAndTypes(new RangeGroup[]{ rangeGroup }, e);
    }
}
