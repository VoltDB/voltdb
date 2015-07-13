/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.types.RowType;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of table conversion.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.1.0
 * @since 2.0.0
 */
public class ExpressionTable extends Expression {

    boolean isTable;
    boolean ordinality = false;

    /**
     * Creates an UNNEST ARRAY or MULTISET expression
     */
    ExpressionTable(Expression[] e, boolean ordinality) {

        super(OpTypes.TABLE);

        nodes           = e;
        this.ordinality = ordinality;
    }

    public String getSQL() {

        if (isTable) {
            return Tokens.T_TABLE;
        } else {
            return Tokens.T_UNNEST;
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        if (isTable) {
            sb.append(Tokens.T_TABLE).append(' ');
        } else {
            sb.append(Tokens.T_UNNEST).append(' ');
        }

        sb.append(nodes[LEFT].describe(session, blanks));

        return sb.toString();
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes.length == 1) {
            if (nodes[LEFT].dataType.isRowType()) {
                if (ordinality) {
                    throw Error.error(ErrorCode.X_42581, Tokens.T_ORDINALITY);
                }

                nodeDataTypes =
                    ((RowType) nodes[LEFT].dataType).getTypesArray();

                table.prepareTable();

                table.columnList =
                    ((FunctionSQLInvoked) nodes[LEFT]).routine.getTable()
                        .columnList;
                isTable = true;

                return;
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            if (!nodes[i].dataType.isArrayType()) {
                throw Error.error(ErrorCode.X_42563, Tokens.T_UNNEST);
            }
        }

        int columnCount = ordinality ? nodes.length + 1
                                     : nodes.length;

        nodeDataTypes = new Type[columnCount];

        for (int i = 0; i < nodes.length; i++) {
            nodeDataTypes[i] = nodes[i].dataType.collectionBaseType();

            if (nodeDataTypes[i] == null
                    || nodeDataTypes[i] == Type.SQL_ALL_TYPES) {
                throw Error.error(ErrorCode.X_42567, Tokens.T_UNNEST);
            }
        }

        if (ordinality) {
            nodeDataTypes[nodes.length] = Type.SQL_INTEGER;
        }

        table.prepareTable();
    }

    public Result getResult(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                RowSetNavigatorData navigator = table.getNavigator(session);
                Result              result    = Result.newResult(navigator);

                result.metaData = table.queryExpression.getMetaData();

                return result;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionTable");
            }
        }
    }

    public Object[] getRowValue(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                return table.queryExpression.getValues(session);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    Object getValue(Session session, Type type) {

        switch (opType) {

            case OpTypes.TABLE : {
                materialise(session);

                Object[] value = table.getValues(session);

                if (value.length == 1) {
                    return ((Object[]) value)[0];
                }

                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    public Object getValue(Session session) {
        return valueData;
    }

    void insertValuesIntoSubqueryTable(Session session,
                                       PersistentStore store) {

        if (isTable) {
            insertTableValues(session, store);
        } else {
            insertArrayValues(session, store);
        }
    }

    private void insertTableValues(Session session, PersistentStore store) {

        Result          result = nodes[LEFT].getResult(session);
        RowSetNavigator nav    = result.navigator;
        int             size   = nav.getSize();

        while (nav.hasNext()) {
            Object[] data = nav.getNext();
            Row row = (Row) store.getNewCachedObject(session, data, false);

            try {
                store.indexRow(session, row);
            } catch (HsqlException e) {}
        }
    }

    private void insertArrayValues(Session session, PersistentStore store) {

        Object[][] array = new Object[nodes.length][];

        for (int i = 0; i < array.length; i++) {
            Object[] values = (Object[]) nodes[i].getValue(session);

            if (values == null) {
                values = ValuePool.emptyObjectArray;
            }

            array[i] = values;
        }

        for (int i = 0; ; i++) {
            boolean  isRow = false;
            Object[] data  = new Object[nodeDataTypes.length];

            for (int arrayIndex = 0; arrayIndex < array.length; arrayIndex++) {
                if (i < array[arrayIndex].length) {
                    data[arrayIndex] = array[arrayIndex][i];
                    isRow            = true;
                }
            }

            if (!isRow) {
                break;
            }

            if (ordinality) {
                data[nodes.length] = ValuePool.getInt(i + 1);
            }

            Row row = (Row) store.getNewCachedObject(session, data, false);

            store.indexRow(session, row);
        }
    }
    // A VoltDB extension to print HSQLDB ASTs
    protected String voltDescribe(Session session, int blanks) {
        StringBuffer sb = new StringBuffer(64);
        if (isTable) {
            sb.append(Tokens.T_TABLE).append(' ');
        } else {
            sb.append(Tokens.T_UNNEST).append(' ');
        }

        sb.append(nodes[LEFT].voltDescribe(session, blanks));

        return sb.toString();
    }
    // End of VoltDB extension

}
