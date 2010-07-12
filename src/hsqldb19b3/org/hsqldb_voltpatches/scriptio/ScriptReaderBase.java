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


package org.hsqldb_voltpatches.scriptio;

import java.io.IOException;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.persist.PersistentStore;

/**
 * Base class for all script readers.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public abstract class ScriptReaderBase {

    public static ScriptReaderBase newScriptReader(Database db, String file,
            int scriptType) throws IOException {

        if (scriptType == ScriptWriterBase.SCRIPT_TEXT_170) {
            return new ScriptReaderText(db, file);
        } else if (scriptType == ScriptWriterBase.SCRIPT_BINARY_172) {
            return new ScriptReaderBinary(db, file);
        } else {
            return new ScriptReaderZipped(db, file);
        }
    }

    public static final int ANY_STATEMENT        = 1;
    public static final int DELETE_STATEMENT     = 2;
    public static final int INSERT_STATEMENT     = 3;
    public static final int SEQUENCE_STATEMENT   = 4;
    public static final int COMMIT_STATEMENT     = 5;
    public static final int SESSION_ID           = 6;
    public static final int SET_SCHEMA_STATEMENT = 7;
    Database                db;
    int                     lineCount;

//    int         byteCount;
    String fileName;

    ScriptReaderBase(Database db,
                     String file) throws IOException {

        this.db  = db;
        fileName = file;

        openFile();
    }

    protected abstract void openFile() throws IOException;

    public void readAll(Session session) throws IOException {
        readDDL(session);
        readExistingData(session);
    }

    protected abstract void readDDL(Session session)
    throws IOException;

    protected abstract void readExistingData(Session session)
    throws IOException;

    public abstract boolean readLoggedStatement(Session session)
    throws IOException;

    int             statementType;
    int             sessionNumber;
    Object[]        rowData;
    long            sequenceValue;
    String          statement;
    Table           currentTable;
    PersistentStore currentStore;
    NumberSequence  currentSequence;
    String          currentSchema;

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
