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


package org.hsqldb_voltpatches.scriptio;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.persist.PersistentStore;

/**
 * Base class for all script readers.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.7.2
 */
public abstract class ScriptReaderBase {

    public static final int ANY_STATEMENT        = 1;
    public static final int DELETE_STATEMENT     = 2;
    public static final int INSERT_STATEMENT     = 3;
    public static final int COMMIT_STATEMENT     = 4;
    public static final int SESSION_ID           = 5;
    public static final int SET_SCHEMA_STATEMENT = 6;
    Database                database;
    int                     lineCount;

    ScriptReaderBase(Database db) {
        this.database = db;
    }

    public void readAll(Session session) {
        readDDL(session);
        readExistingData(session);
    }

    protected abstract void readDDL(Session session);

    protected abstract void readExistingData(Session session);

    public abstract boolean readLoggedStatement(Session session);

    int              statementType;
    int              sessionNumber;
    boolean          sessionChanged;
    Object[]         rowData;
    long             sequenceValue;
    String           rawStatement;
    String           statement;
    Table            currentTable;
    PersistentStore  currentStore;
    NumberSequence   currentSequence;
    String           currentSchema;
    ScriptWriterText scrwriter;

    public int getStatementType() {
        return statementType;
    }

    public int getSessionNumber() {
        return sessionNumber;
    }

    public Object[] getData() {
        return rowData;
    }

    public String getLoggedStatement() {
        return statement;
    }

    public NumberSequence getCurrentSequence() {
        return currentSequence;
    }

    public long getSequenceValue() {
        return sequenceValue;
    }

    public Table getCurrentTable() {
        return currentTable;
    }

    public String getCurrentSchema() {
        return currentSchema;
    }

    public int getLineNumber() {
        return lineCount;
    }

    public abstract void close();
}
