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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;

/**
 * Base class for compiled statement objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public abstract class Statement {

    final static int META_RESET_VIEWS      = 1;
    final static int META_RESET_STATEMENTS = 2;

    //
    final static Statement[] emptyArray = new Statement[]{};

    //
    final int type;
    int       group;
    boolean   isLogged = true;
    boolean   isValid = true;

    /** the default schema name used to resolve names in the sql */
    HsqlName schemaName;

    /** root in PSM */
    Routine root;

    /** parent in PSM */
    StatementCompound parent;
    boolean           isError;
    boolean           isTransactionStatement;
    boolean           isExplain;
    int               metaDataImpact;

    /** SQL string for the statement */
    String sql;

    /** id in StatementManager */
    long id;

    /** table names read - for concurrency control */
    HsqlName[] readTableNames = HsqlName.emptyArray;

    /** table names written - for concurrency control */
    HsqlName[] writeTableNames = HsqlName.emptyArray;;

    public abstract Result execute(Session session);

    public void setParameters(ExpressionColumn[] params) {}

    Statement(int type) {
        this.type = type;
    }

    Statement(int type, int group) {
        this.type  = type;
        this.group = group;
    }

    public final boolean isError() {
        return isError;
    }

    public boolean isTransactionStatement() {
        return isTransactionStatement;
    }

    public boolean isAutoCommitStatement() {
        return false;
    }

    public final void setSQL(String sql) {
        this.sql = sql;
    }

    public String getSQL() {
        return sql;
    }

    public final void setDescribe() {
        isExplain = true;
    }

    public abstract String describe(Session session);

    public HsqlName getSchemaName() {
        return schemaName;
    }

    public final void setSchemaHsqlName(HsqlName name) {
        schemaName = name;
    }

    public final void setID(long csid) {
        id = csid;
    }

    public final long getID() {
        return id;
    }

    public final int getType() {
        return type;
    }

    public final int getGroup() {
        return group;
    }

    public final boolean isValid() {
        return isValid;
    }

    public final boolean isLogged() {
        return isLogged;
    }

    public void clearVariables() {}

    public void resolve() {}

    public RangeVariable[] getRangeVariables() {
        return RangeVariable.emptyArray;
    }

    void getTableNamesForRead(OrderedHashSet set) {}

    void getTableNamesForWrite(OrderedHashSet set) {}

    public final HsqlName[] getTableNamesForRead() {
        return readTableNames;
    }

    public final HsqlName[] getTableNamesForWrite() {
        return writeTableNames;
    }

    public void setParent(StatementCompound statement) {
        this.parent = statement;
    }

    public void setRoot(Routine root) {
        this.root = root;
    }

    public boolean hasGeneratedColumns() {
        return false;
    }

    public ResultMetaData generatedResultMetaData() {
        return null;
    }

    public void setGeneratedColumnInfo(int mode, ResultMetaData meta) {}

    public ResultMetaData getResultMetaData() {
        return ResultMetaData.emptyResultMetaData;
    }

    public ResultMetaData getParametersMetaData() {
        return ResultMetaData.emptyParamMetaData;
    }

    /************************* Volt DB Extensions *************************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetStatementXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                "this type of sql statement is not supported or is not allowed in this context");
    }
    /**********************************************************************/
}
