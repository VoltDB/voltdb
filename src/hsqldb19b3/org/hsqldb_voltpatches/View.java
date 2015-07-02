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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.OrderedHashSet;

// fredt@users 20020420 - patch523880 by leptipre@users - VIEW support - modified
// fredt@users 20031227 - remimplementated as compiled query

/**
 * Represents an SQL VIEW based on a query expression
 *
 * @author leptipre@users
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.7.0
 */
public class View extends TableDerived {

    private String statement;

    //
    private HsqlName[] columnNames;

    /**
     * Names of SCHEMA objects referenced in VIEW
     */
    private OrderedHashSet schemaObjectNames;

    /**
     * check option
     */
    private int checkOption;

    //
    private Table baseTable;

    //
    boolean isTriggerInsertable;
    boolean isTriggerUpdatable;
    boolean isTriggerDeletable;

    View(Database db, HsqlName name, HsqlName[] columnNames, int check) {

        super(db, name, TableBase.VIEW_TABLE);

        this.columnNames = columnNames;
        this.checkOption = check;
    }

    public int getType() {
        return SchemaObject.VIEW;
    }

    public OrderedHashSet getReferences() {
        return schemaObjectNames;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    /**
     * Compiles the query expression and sets up the columns.
     */
    public void compile(Session session, SchemaObject parentObject) {

        ParserDQL p = new ParserDQL(session, new Scanner(statement), null);

        p.read();

        TableDerived viewSubQueryTable = p.XreadViewSubqueryTable(this, true);

        queryExpression = viewSubQueryTable.queryExpression;

        if (getColumnCount() == 0) {
            if (columnNames == null) {
                columnNames =
                    viewSubQueryTable.queryExpression.getResultColumnNames();
            }

            if (columnNames.length
                    != viewSubQueryTable.queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593, getName().statementName);
            }

            TableUtil.setColumnsInSchemaTable(
                this, columnNames, queryExpression.getColumnTypes());
        }

        //
        schemaObjectNames = p.compileContext.getSchemaObjectNames();
        canRecompile      = true;
        baseTable         = queryExpression.getBaseTable();

        if (baseTable == null) {
            return;
        }

        switch (checkOption) {

            case SchemaObject.ViewCheckModes.CHECK_NONE :
            case SchemaObject.ViewCheckModes.CHECK_LOCAL :
            case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_VIEW);
        sb.append(' ');
        sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
        sb.append('(');

        int count = getColumnCount();

        for (int j = 0; j < count; j++) {
            sb.append(getColumn(j).getName().statementName);

            if (j < count - 1) {
                sb.append(',');
            }
        }

        sb.append(')').append(' ').append(Tokens.T_AS).append(' ');
        sb.append(getStatement());

        return sb.toString();
    }

    public int[] getUpdatableColumns() {
        return queryExpression.getBaseTableColumnMap();
    }

    public boolean isTriggerInsertable() {
        return isTriggerInsertable;
    }

    public boolean isTriggerUpdatable() {
        return isTriggerUpdatable;
    }

    public boolean isTriggerDeletable() {
        return isTriggerDeletable;
    }

    public boolean isInsertable() {
        return isTriggerInsertable ? false
                                   : super.isInsertable();
    }

    public boolean isUpdatable() {
        return isTriggerUpdatable ? false
                                  : super.isUpdatable();
    }

    void addTrigger(TriggerDef td, HsqlName otherName) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                if (isTriggerInsertable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerInsertable = true;
                break;

            case StatementTypes.DELETE_WHERE :
                if (isTriggerDeletable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerDeletable = true;
                break;

            case StatementTypes.UPDATE_WHERE :
                if (isTriggerUpdatable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerUpdatable = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.addTrigger(td, otherName);
    }

    void removeTrigger(TriggerDef td) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                isTriggerInsertable = false;
                break;

            case StatementTypes.DELETE_WHERE :
                isTriggerDeletable = false;
                break;

            case StatementTypes.UPDATE_WHERE :
                isTriggerUpdatable = false;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.removeTrigger(td);
    }

    /**
     * Overridden to disable SET TABLE READONLY DDL for View objects.
     */
    public void setDataReadOnly(boolean value) {
        throw Error.error(ErrorCode.X_28000);
    }

    public int getCheckOption() {
        return checkOption;
    }

    /**
     * Returns the query expression for the view.
     */
    public String getStatement() {
        return statement;
    }

    public void setStatement(String sql) {
        statement = sql;
    }

    public TableDerived newDerivedTable(Session session) {

        TableDerived td;
        ParserDQL p = new ParserDQL(session, new Scanner(),
                                    session.parser.compileContext);

        p.reset(statement);
        p.read();

        td = p.XreadViewSubqueryTable(this, false);

        return td;
    }
    // A VoltDB extension to export abstract parse trees

    /**
     * VoltDB added method to get an abstract parse tree
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return VoltXMLElement, a free-form (schema-less) abstract parse tree.
     * @throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
     */
    VoltXMLElement voltGetTableXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement table = super.voltGetTableXML(session);

        // add view metadata
        table.attributes.put("query", statement);
        return table;
    }
    // End of VoltDB extension
}
