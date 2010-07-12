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


package org.hsqldb_voltpatches.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * @author Nicolas BAZIN, INGENICO
 * @version 1.7.0
 */
class TransferSQLText extends DataAccessPoint {

    String              sFileName              = null;
    BufferedWriter      WTextWrite             = null;
    BufferedReader      WTextRead              = null;
    protected boolean   StructureAlreadyParsed = false;
    Hashtable           DbStmts                = null;
    protected JDBCTypes JDBCT                  = null;

    TransferSQLText(String _FileName,
                    Traceable t) throws DataAccessPointException {

        super(t);

        sFileName = _FileName;
        JDBCT     = new JDBCTypes();

        if (sFileName == null) {
            throw new DataAccessPointException("File name not initialized");
        }
    }

    boolean execute(String statement) throws DataAccessPointException {

        if (WTextWrite == null) {
            try {
                WTextWrite = new BufferedWriter(new FileWriter(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        try {
            WTextWrite.write(statement + "\n");
            WTextWrite.flush();
        } catch (IOException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        return true;
    }

    void putData(String statement, TransferResultSet r,
                 int iMaxRows) throws DataAccessPointException {

        int i = 0;

        if (r == null) {
            return;
        }

        if (WTextWrite == null) {
            try {
                WTextWrite = new BufferedWriter(new FileWriter(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        try {
            while (r.next()) {
                if (i == 0) {
                    WTextWrite.write(statement + "\n");
                    WTextWrite.flush();
                }

                transferRow(r);

                if (iMaxRows != 0 && i == iMaxRows) {
                    break;
                }

                i++;

                if (iMaxRows != 0 || i % 100 == 0) {
                    tracer.trace("Transfered " + i + " rows");
                }
            }
        } catch (Exception e) {
            throw new DataAccessPointException(e.getMessage());
        } finally {
            try {
                if (i > 0) {
                    WTextWrite.write("\tNumber of Rows=" + i + "\n\n");
                    WTextWrite.flush();
                }
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }
    }

    void close() throws DataAccessPointException {

        if (WTextWrite != null) {
            try {
                WTextWrite.flush();
                WTextWrite.close();
            } catch (IOException e) {}
        }
    }

    /**
     * Method declaration
     *
     *
     * @param type
     * @param r
     * @param p
     *
     * @throws SQLException
     */
    private void transferRow(TransferResultSet r) throws Exception {

        String sLast = "";
        int    len   = r.getColumnCount();

        if (WTextWrite == null) {
            try {
                WTextWrite = new BufferedWriter(new FileWriter(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        for (int i = 0; i < len; i++) {
            int t = r.getColumnType(i + 1);

            sLast = "column=" + r.getColumnName(i + 1) + " datatype="
                    + (String) helper.getSupportedTypes().get(new Integer(t));

            Object o = r.getObject(i + 1);

            if (o == null) {
                sLast += " value=<null>";
            } else {
                o     = helper.convertColumnValue(o, i + 1, t);
                sLast += " value=\'" + o.toString() + "\'";
            }

            WTextWrite.write("\t" + sLast + "\n");
            WTextWrite.flush();
        }

        WTextWrite.write("\n");
        WTextWrite.flush();

        sLast = "";
    }

    class ColumnDef {

        String columnName;
        String columnType;
        String options;
        int    start;
        int    len;

        public ColumnDef() {

            columnName = "";
            columnType = "";
            options    = "";
            start      = 0;
            len        = 0;
        }
    }

    ColumnDef getColumnDef(String ColumnsDesc, int curPos) {

        int       nextPos   = 0;
        ColumnDef columnDef = new TransferSQLText.ColumnDef();

        columnDef.start = curPos;

        if ((ColumnsDesc == null) || (ColumnsDesc.length() == 0)
                || (curPos >= ColumnsDesc.length())) {
            return new TransferSQLText.ColumnDef();
        }

        String stbuff = ColumnsDesc.substring(curPos);

        try {
            int i = 0;

            for (; i < stbuff.length(); i++) {
                int c = stbuff.charAt(i);

                if (c == ',' || c == ' ' || c == ')' || c == ';') {
                    continue;
                } else {
                    break;
                }
            }

            if (i == stbuff.length()) {
                return new TransferSQLText.ColumnDef();
            }

            columnDef.len += i;
            stbuff        = stbuff.substring(i);

            while (stbuff.charAt(nextPos) != ' ') {
                nextPos++;
            }

            columnDef.columnName = stbuff.substring(0, nextPos);
            stbuff               = stbuff.substring(nextPos);
            columnDef.len        += nextPos;
            nextPos              = 0;

            if (!columnDef.columnName.toUpperCase().equals("CONSTRAINT")) {
                i = 0;

                for (; i < stbuff.length() && stbuff.charAt(i) == ' '; i++) {}

                stbuff        = stbuff.substring(i);
                columnDef.len += i;

                while ((stbuff.charAt(nextPos) != '(')
                        && (stbuff.charAt(nextPos) != ',')
                        && (stbuff.charAt(nextPos) != ')')
                        && (stbuff.charAt(nextPos) != ';')
                        && (stbuff.charAt(nextPos) != ' ')) {
                    nextPos++;
                }

                columnDef.columnType = stbuff.substring(0,
                        nextPos).toUpperCase();
                stbuff        = stbuff.substring(nextPos);
                columnDef.len += nextPos;
                nextPos       = 0;
            }

            while ((stbuff.charAt(nextPos) != ',')
                    && (stbuff.charAt(nextPos) != ';')
                    && (nextPos < stbuff.length())
                    && (stbuff.charAt(nextPos) != ')')) {
                if (stbuff.charAt(nextPos) == '(') {
                    while (stbuff.charAt(nextPos) != ')') {
                        nextPos++;
                    }
                }

                nextPos++;
            }

            columnDef.options = stbuff.substring(0, nextPos);
            columnDef.len     += nextPos;
        } catch (Exception e) {
            columnDef = new TransferSQLText.ColumnDef();
        }

        return columnDef;
    }

    String translateTypes(String CreateLine, TransferTable TTable,
                          DataAccessPoint Dest)
                          throws DataAccessPointException {

        String    translatedLine = "";
        JDBCTypes JDBCT          = new JDBCTypes();
        int       currentPos     = 0;
        String    columnName     = "";
        String    columnType     = "";
        int       colnum         = 0;
        ColumnDef cDef;

        currentPos     = CreateLine.indexOf('(') + 1;
        translatedLine = CreateLine.substring(0, currentPos);

        do {
            cDef = getColumnDef(CreateLine, currentPos);

            if (cDef.len == 0) {
                break;
            }

            columnName = cDef.columnName;
            columnType = cDef.columnType;

            if (columnName.toUpperCase().indexOf("CONSTRAINT") >= 0) {
                translatedLine +=
                    CreateLine.substring(currentPos, currentPos + cDef.len)
                    + ",";
                currentPos += cDef.len + 1;

                colnum++;

                continue;
            }

            columnName = Dest.helper.formatIdentifier(columnName) + " ";

            try {
                Integer inttype = new Integer(
                    Dest.helper.convertToType(JDBCT.toInt(columnType)));

                columnType = (String) TTable.hTypes.get(inttype);
            } catch (Exception JDBCtypeEx) {}

            if (cDef.options != null) {
                columnType += cDef.options;
            }

            try {
                columnType = Dest.helper.fixupColumnDefWrite(TTable, null,
                        columnType, null, colnum);
            } catch (SQLException SQLe) {
                return CreateLine;
            }

            translatedLine += columnName + " " + columnType + ",";
            currentPos     += cDef.len + 1;

            colnum++;
        } while (true);

        return translatedLine.substring(0, translatedLine.length() - 1)
               + ");";
    }

    void parseFileForTables() throws DataAccessPointException {

        StringTokenizer Tokenizer;

        if (WTextRead == null) {
            try {
                WTextRead = new BufferedReader(new FileReader(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        String        currentLine  = "";
        String        Token        = "";
        String        name         = "";
        TransferTable relatedTable = null;

        try {
            while ((currentLine = WTextRead.readLine()) != null) {
                currentLine = currentLine.trim() + ";";
                Tokenizer   = new StringTokenizer(currentLine);

                try {
                    Token = Tokenizer.nextToken();
                } catch (NoSuchElementException NSE) {
                    continue;
                }

                if (Token == null) {
                    continue;
                }

                if (!Token.toUpperCase().equals("CREATE")) {
                    continue;
                }

                Token = Tokenizer.nextToken().toUpperCase();

                if (Token.equals("TABLE") || Token.equals("VIEW")) {
                    try {
                        name = Tokenizer.nextToken(" (;");
                        relatedTable = new TransferTable(this, name, "",
                                                         Token, tracer);
                        relatedTable.Stmts.bCreate      = false;
                        relatedTable.Stmts.bDelete      = false;
                        relatedTable.Stmts.bDrop        = false;
                        relatedTable.Stmts.bCreateIndex = false;
                        relatedTable.Stmts.bDropIndex   = false;
                        relatedTable.Stmts.bInsert      = false;
                        relatedTable.Stmts.bAlter       = false;

                        DbStmts.put(relatedTable.Stmts.sSourceTable,
                                    relatedTable);
                    } catch (NoSuchElementException NSE) {
                        continue;
                    }
                }
            }
        } catch (Exception IOe) {
            throw new DataAccessPointException(IOe.getMessage());
        }
    }

    void parseFileForTheRest(TransferTable TTable,
                             DataAccessPoint Dest)
                             throws DataAccessPointException {

        StringTokenizer Tokenizer;

        StructureAlreadyParsed = true;

        if (WTextRead == null) {
            try {
                WTextRead = new BufferedReader(new FileReader(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        String        currentLine  = "";
        String        Token        = "";
        String        name         = "";
        TransferTable relatedTable = null;

        try {
            while ((currentLine = WTextRead.readLine()) != null) {
                currentLine = currentLine.trim() + ";";
                Tokenizer   = new StringTokenizer(currentLine);

                try {
                    Token = Tokenizer.nextToken();
                } catch (NoSuchElementException NSE) {
                    continue;
                }

                if (Token == null) {
                    continue;
                }

                if (Token.toUpperCase().equals("INSERT")) {
                    try {
                        if (!Tokenizer.nextToken().toUpperCase().equals(
                                "INTO")) {
                            throw new DataAccessPointException(
                                "Error in INSERT statement: no INTO found");
                        }

                        Token = Tokenizer.nextToken();

                        if ((relatedTable =
                                (TransferTable) DbStmts.get(Token)) != null) {
                            relatedTable.Stmts.bDelete     = true;
                            relatedTable.Stmts.bInsert     = true;
                            relatedTable.Stmts.sDestInsert = currentLine;
                            relatedTable.Stmts.sDestDelete =
                                "DELETE FROM "
                                + relatedTable.Stmts.sSourceTable + ";";
                        }

                        continue;
                    } catch (NoSuchElementException NSE) {
                        continue;
                    }
                } else if (Token.toUpperCase().equals("ALTER")) {
                    try {
                        if (!Tokenizer.nextToken().toUpperCase().equals(
                                "TABLE")) {
                            continue;
                        }

                        name  = Tokenizer.nextToken();
                        Token = Tokenizer.nextToken().toUpperCase();

                        if (!Token.equals("ADD")) {
                            continue;
                        }

                        do {
                            Token = Tokenizer.nextToken().toUpperCase();
                        } while (!Token.equals("CONSTRAINT"));

                        if ((relatedTable = (TransferTable) DbStmts.get(name))
                                != null) {
                            if (relatedTable.Stmts.sDestAlter == null) {
                                relatedTable.Stmts.sDestAlter = "";
                            }

                            relatedTable.Stmts.bAlter     = true;
                            relatedTable.Stmts.sDestAlter += currentLine;
                        } else {
                            throw new DataAccessPointException(
                                "table not found");
                        }

                        Token = Tokenizer.nextToken();

                        if (relatedTable.Stmts.sDestDrop == null) {
                            relatedTable.Stmts.sDestDrop = "";
                        }

                        relatedTable.Stmts.bDrop = true;
                        relatedTable.Stmts.sDestDrop =
                            "ALTER TABLE " + name + " DROP CONSTRAINT "
                            + Token + ";" + relatedTable.Stmts.sDestDrop;

                        continue;
                    } catch (NoSuchElementException NSE) {
                        continue;
                    }
                } else if (!Token.toUpperCase().equals("CREATE")) {
                    continue;
                }

                Token = Tokenizer.nextToken().toUpperCase();

                if (Token.equals("TABLE") || Token.equals("VIEW")) {
                    try {
                        name = Tokenizer.nextToken(" (;");

                        if (!DbStmts.containsKey(name)) {
                            throw new DataAccessPointException(
                                "error: index is created before the table");
                        }

                        relatedTable = (TransferTable) DbStmts.get(name);
                        relatedTable.Stmts.bCreate = true;
                        relatedTable.Stmts.bDrop   = true;

//                        relatedTable.Stmts.sDestCreate = currentLine;
                        relatedTable.Stmts.sDestCreate =
                            translateTypes(currentLine, TTable, Dest);
                        relatedTable.Stmts.sDestDrop =
                            "DROP " + relatedTable.Stmts.sType + " " + name
                            + ";";

                        DbStmts.put(relatedTable.Stmts.sSourceTable,
                                    relatedTable);
                    } catch (NoSuchElementException NSE) {
                        continue;
                    }
                }

                if (Token.equals("INDEX") || Token.equals("UNIQUE")) {
                    try {
                        while ((Token =
                                Tokenizer.nextToken()).toUpperCase().equals(
                                    "INDEX")) {
                            ;
                        }

                        String IndexdropCommand = "DROP INDEX " + Token
                                                  + " ;";

                        while ((Token = Tokenizer.nextToken(
                                " (")).toUpperCase().equals("ON")) {
                            ;
                        }

                        name = Token;

                        if (!DbStmts.containsKey(Token)) {
                            throw new DataAccessPointException(
                                "error: index is created before the table");
                        }

                        relatedTable = (TransferTable) DbStmts.get(Token);

                        if (relatedTable.Stmts.sDestCreateIndex == null) {
                            relatedTable.Stmts.sDestCreateIndex = "";
                        }

                        if (relatedTable.Stmts.sDestDropIndex == null) {
                            relatedTable.Stmts.sDestDropIndex = "";
                        }

                        relatedTable.Stmts.bCreateIndex     = true;
                        relatedTable.Stmts.bDropIndex       = true;
                        relatedTable.Stmts.sDestCreateIndex += currentLine;
                        relatedTable.Stmts.sDestDropIndex += IndexdropCommand;
                    } catch (NoSuchElementException NSE) {
                        continue;
                    }
                }
            }
        } catch (IOException IOe) {
            throw new DataAccessPointException(IOe.getMessage());
        }
    }

    Vector getTables(String sCatalog,
                     String[] sSchemas) throws DataAccessPointException {

        Vector AllTables = new Vector();

        if (DbStmts == null) {
            DbStmts = new Hashtable();
        }

        if (WTextRead != null) {
            try {
                WTextRead.close();

                WTextRead = null;
            } catch (IOException e) {}
        }

        this.parseFileForTables();

        StructureAlreadyParsed = false;

        Enumeration e = DbStmts.elements();

        while (e.hasMoreElements()) {
            AllTables.addElement(e.nextElement());
        }

        return AllTables;
    }

    void getTableStructure(TransferTable TTable,
                           DataAccessPoint Dest)
                           throws DataAccessPointException {

        if (!StructureAlreadyParsed) {
            if (WTextRead != null) {
                try {
                    WTextRead.close();

                    WTextRead = null;
                } catch (IOException e) {}
            }

            this.parseFileForTheRest(TTable, Dest);
        }
    }

    TransferResultSet getData(String statement)
    throws DataAccessPointException {

        StringTokenizer Tokenizer;
        String          tableName = "";

        try {
            Tokenizer = new StringTokenizer(statement);

            while (!Tokenizer.nextToken().toUpperCase().equals("FROM")) {
                ;
            }

            tableName = Tokenizer.nextToken(" ;");
        } catch (NoSuchElementException NSE) {
            throw new DataAccessPointException(
                "Table name not found in statement: " + statement);
        }

        if (WTextRead != null) {
            try {
                WTextRead.close();

                WTextRead = null;
            } catch (IOException e) {}
        }

        return (this.parseFileForData(tableName));
    }

    TransferResultSet parseFileForData(String tableName)
    throws DataAccessPointException {

        TransferResultSet trsData = new TransferResultSet();
        StringTokenizer   Tokenizer;

        if (WTextRead == null) {
            try {
                WTextRead = new BufferedReader(new FileReader(sFileName));
            } catch (IOException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }

        String currentLine = "";
        String Token;

        try {
            while ((currentLine = WTextRead.readLine()) != null) {
                currentLine = currentLine.trim() + ";";
                Tokenizer   = new StringTokenizer(currentLine);

                try {
                    Token = Tokenizer.nextToken();
                } catch (NoSuchElementException NSE) {
                    continue;
                }

                if (Token == null) {
                    continue;
                }

                if (!Token.toUpperCase().equals("INSERT")) {
                    continue;
                }

                try {
                    if (!Tokenizer.nextToken().toUpperCase().equals("INTO")) {
                        throw new DataAccessPointException(
                            "Error in INSERT statement: no INTO found");
                    }

                    Token = Tokenizer.nextToken();

                    if (!Token.equals(tableName)) {
                        continue;
                    }

                    int    iParsedRows   = 0;
                    Vector vColumnNames  = new Vector();
                    Vector vColumnValues = new Vector();
                    Vector vColumnTypes  = new Vector();

                    while ((currentLine = WTextRead.readLine()) != null) {
                        currentLine = currentLine.trim();

                        boolean newLine = (currentLine.length() == 0);

                        if (newLine) {
                            int iColumnNb = 0;

                            iParsedRows++;

                            iColumnNb = vColumnNames.size();

                            String[] Names  = new String[iColumnNb + 1];
                            int[]    Types  = new int[iColumnNb + 1];
                            Object[] Values = new Object[iColumnNb + 1];

                            for (int Idx = 0; Idx < iColumnNb; Idx++) {
                                Names[Idx + 1] =
                                    (String) vColumnNames.elementAt(Idx);
                                Types[Idx + 1] =
                                    ((Integer) vColumnTypes.elementAt(
                                        Idx)).intValue();
                                Values[Idx + 1] =
                                    vColumnValues.elementAt(Idx);
                            }

                            try {
                                trsData.addRow(Names, Types, Values,
                                               iColumnNb);
                            } catch (Exception e) {
                                throw new DataAccessPointException(
                                    e.getMessage());
                            }

                            iColumnNb = 0;

                            vColumnNames.removeAllElements();
                            vColumnValues.removeAllElements();
                            vColumnTypes.removeAllElements();

                            continue;
                        }

                        Tokenizer = new StringTokenizer(currentLine);
                        Token     = Tokenizer.nextToken("=");

                        if (Token.equals("Number of Rows")) {
                            int iNbRows =
                                Integer.parseInt(Tokenizer.nextToken());

                            if (iNbRows != iParsedRows) {
                                throw new DataAccessPointException(
                                    "Number of parsed rows (" + iParsedRows
                                    + ") is different from the expected ("
                                    + iNbRows + ")");
                            }

                            return trsData;
                        }

                        if (Token.equals("column")) {
                            Token = Tokenizer.nextToken(" =");

                            vColumnNames.addElement(Token);
                        }

                        Token = Tokenizer.nextToken(" =");

                        if (Token.equals("datatype")) {
                            int iType;

                            Token = Tokenizer.nextToken(" =");

                            try {
                                iType = JDBCT.toInt(Token.toUpperCase());
                            } catch (Exception e) {
                                throw new DataAccessPointException(
                                    "Unknown type: " + Token);
                            }

                            vColumnTypes.addElement(new Integer(iType));
                        }

                        Token = Tokenizer.nextToken(" =");

                        if (Token.equals("value")) {
                            int iStart = currentLine.indexOf("value=") + 6;
                            String sValue =
                                currentLine.substring(iStart).trim();

                            if (sValue.indexOf("<null>") >= 0) {
                                vColumnValues.addElement(null);
                            } else {
                                int    i       = sValue.indexOf('\'') + 1;
                                String sbToken = sValue.substring(i);

                                i       = sbToken.lastIndexOf('\'');
                                sbToken = sbToken.substring(0, i);
                                Token   = sbToken;

                                vColumnValues.addElement(Token);
                            }
                        }
                    }
                } catch (IndexOutOfBoundsException IOBe) {
                    continue;
                }
            }
        } catch (IOException IOe) {
            throw new DataAccessPointException(IOe.getMessage());
        }

        return trsData;
    }
}
