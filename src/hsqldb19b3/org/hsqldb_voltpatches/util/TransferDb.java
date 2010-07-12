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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Vector;

// fredt@users 20020215 - patch 516309 by Nicolas Bazin - enhancements
// sqlbob@users 20020401 - patch 1.7.0 - reengineering
// nicolas BAZIN 20020430 - add support of Catalog and mckoi db helper
// Stephan Frind 20040508 - speed improvements

/**
 * Conversions between different databases
 *
 * @version 1.7.0
 */
class TransferDb extends DataAccessPoint {

    Connection          conn;
    DatabaseMetaData    meta;
    protected Statement srcStatement = null;

    TransferDb(Connection c, Traceable t) throws DataAccessPointException {

        super(t);

        conn = c;

        if (c != null) {
            String productLowerName;

            try {
                meta              = c.getMetaData();
                databaseToConvert = c.getCatalog();
                productLowerName  = meta.getDatabaseProductName();

                if (productLowerName == null) {
                    productLowerName = "";
                } else {
                    productLowerName = productLowerName.toLowerCase();
                }

                helper = HelperFactory.getHelper(productLowerName);

                helper.set(this, t, meta.getIdentifierQuoteString());
            } catch (SQLException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }
    }

    boolean isConnected() {
        return (conn != null);
    }

    boolean getAutoCommit() throws DataAccessPointException {

        boolean result = false;

        try {
            result = conn.getAutoCommit();
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        return result;
    }

    void commit() throws DataAccessPointException {

        if (srcStatement != null) {
            try {
                srcStatement.close();
            } catch (SQLException e) {}

            srcStatement = null;
        }

        try {
            conn.commit();
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    void rollback() throws DataAccessPointException {

        if (srcStatement != null) {
            try {
                srcStatement.close();
            } catch (SQLException e) {}

            srcStatement = null;
        }

        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    void setAutoCommit(boolean flag) throws DataAccessPointException {

        try {
            conn.setAutoCommit(flag);
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    boolean execute(String statement) throws DataAccessPointException {

        boolean   result = false;
        Statement stmt   = null;

        try {
            stmt   = conn.createStatement();
            result = stmt.execute(statement);
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {}
            }
        }

        return result;
    }

    TransferResultSet getData(String statement)
    throws DataAccessPointException {

        ResultSet rsData = null;

        try {
            if (srcStatement != null) {
                srcStatement.close();
            }

            srcStatement = conn.createStatement();
            rsData       = srcStatement.executeQuery(statement);
        } catch (SQLException e) {
            try {
                srcStatement.close();
            } catch (Exception e1) {}

            srcStatement = null;
            rsData       = null;

            throw new DataAccessPointException(e.getMessage());
        }

        return new TransferResultSet(rsData);
    }

    void putData(String statement, TransferResultSet r,
                 int iMaxRows) throws DataAccessPointException {

        if ((statement == null) || statement.equals("") || (r == null)) {
            return;
        }

        PreparedStatement destPrep = null;

        try {
            destPrep = conn.prepareStatement(statement);

            int   i = 0;
            int   tmpLength;
            int   len      = r.getColumnCount();
            int[] tmpTypes = null;

            while (r.next()) {
                if (tmpTypes == null) {
                    tmpTypes = new int[len + 1];

                    for (int j = 1; j <= len; j++) {
                        tmpTypes[j] = r.getColumnType(j);
                    }
                }

                transferRow(r, destPrep, len, tmpTypes);

                if (iMaxRows != 0 && i == iMaxRows) {
                    break;
                }

                i++;

                if (iMaxRows != 0 || i % 100 == 0) {
                    tracer.trace("Transfered " + i + " rows");
                }
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        } finally {
            if (destPrep != null) {
                try {
                    destPrep.close();
                } catch (SQLException e) {}
            }
        }
    }

/*
    private void transferRow(TransferResultSet r,
                             PreparedStatement p)
                             throws DataAccessPointException, SQLException {
        // TODO
        // what is this never used variable for?
        // looks like missing debug flags because constructing these strings consumes a lot
        // of time
        String sLast = "";

        if (p != null) {
            p.clearParameters();
        }

        int len = r.getColumnCount();

        for (int i = 0; i < len; i++) {
            int t = r.getColumnType(i + 1);

            sLast = "column=" + r.getColumnName(i + 1) + " datatype="
                    + (String) helper.getSupportedTypes().get(new Integer(t));

            Object o = r.getObject(i + 1);

            if (o == null) {
                if (p != null) {
                    p.setNull(i + 1, t);
                }

                sLast += " value=<null>";
            } else {
                o = helper.convertColumnValue(o, i + 1, t);

                p.setObject(i + 1, o);

                sLast += " value=\'" + o.toString() + "\'";
            }
        }

        if (p != null) {
            p.execute();
        }

        sLast = "";
    }
*/
    Vector getSchemas() throws DataAccessPointException {

        Vector    ret    = new Vector();
        ResultSet result = null;

        try {
            result = meta.getSchemas();
        } catch (SQLException e) {
            result = null;
        }

        try {
            if (result != null) {
                while (result.next()) {
                    ret.addElement(result.getString(1));
                }

                result.close();
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        return (ret);
    }

    Vector getCatalog() throws DataAccessPointException {

        Vector    ret    = new Vector();
        ResultSet result = null;

        if (databaseToConvert != null && databaseToConvert.length() > 0) {
            ret.addElement(databaseToConvert);

            return (ret);
        }

        try {
            result = meta.getCatalogs();
        } catch (SQLException e) {
            result = null;
        }

        try {
            if (result != null) {
                while (result.next()) {
                    ret.addElement(result.getString(1));
                }

                result.close();
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        return (ret);
    }

    void setCatalog(String sCatalog) throws DataAccessPointException {

        if (sCatalog != null && sCatalog.length() > 0) {
            try {
                conn.setCatalog(sCatalog);
            } catch (SQLException e) {
                throw new DataAccessPointException(e.getMessage());
            }
        }
    }

    Vector getTables(String sCatalog,
                     String[] sSchemas) throws DataAccessPointException {

        Vector    tTable = new Vector();
        ResultSet result = null;

        tracer.trace("Reading source tables");

        int nbloops = 1;

        if (sSchemas != null) {
            nbloops = sSchemas.length;
        }

        try {

// variations return null or emtpy result sets with informix JDBC driver 2.2
            for (int SchemaIdx = 0; SchemaIdx < nbloops; SchemaIdx++) {
                if (sSchemas != null && sSchemas[SchemaIdx] != null) {
                    result = meta.getTables(sCatalog, sSchemas[SchemaIdx],
                                            null, null);
                } else {
                    try {
                        result = meta.getTables(sCatalog, "", null, null);
                    } catch (SQLException e) {
                        result = meta.getTables(sCatalog, null, null, null);
                    }
                }

                while (result.next()) {
                    String name   = result.getString(3);
                    String type   = result.getString(4);
                    String schema = "";

                    if (sSchemas != null && sSchemas[SchemaIdx] != null) {
                        schema = sSchemas[SchemaIdx];
                    }

                    /*
                    ** we ignore the following table types:
                    **    "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY"
                    **    "ALIAS", "SYNONYM"
                    */
                    if ((type.compareTo("TABLE") == 0)
                            || (type.compareTo("VIEW") == 0)) {
                        TransferTable t = new TransferTable(this, name,
                                                            schema, type,
                                                            tracer);

                        tTable.addElement(t);
                    } else {
                        tracer.trace("Found table of type :" + type
                                     + " - this type is ignored");
                    }
                }
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {}
            }
        }

        return (tTable);
    }

    void getTableStructure(TransferTable TTable,
                           DataAccessPoint Dest)
                           throws DataAccessPointException {

        String create = "CREATE " + TTable.Stmts.sType + " "
                        + Dest.helper.formatName(TTable.Stmts.sDestTable);
        String    insert         = "";
        ResultSet ImportedKeys   = null;
        boolean   importedkeys   = false;
        String    alterCreate    = new String("");
        String    alterDrop      = new String("");
        String    ConstraintName = new String("");
        String    RefTableName   = new String("");
        String    foreignKeyName = new String("");
        String    columnName     = new String("");

        TTable.Stmts.sDestDrop =
            "DROP " + TTable.Stmts.sType + " "
            + Dest.helper.formatName(TTable.Stmts.sDestTable) + ";";

        if (TTable.Stmts.sType.compareTo("TABLE") == 0) {
            TTable.Stmts.sDestDelete =
                "DELETE FROM "
                + Dest.helper.formatName(TTable.Stmts.sDestTable) + ";";
            create += "(";
        } else if (TTable.Stmts.sType.compareTo("VIEW") == 0) {
            TTable.Stmts.bDelete     = false;
            TTable.Stmts.sDestDelete = "";
            create                   += " AS SELECT ";
        }

        if (TTable.Stmts.sType.compareTo("TABLE") == 0) {
            insert = "INSERT INTO "
                     + Dest.helper.formatName(TTable.Stmts.sDestTable)
                     + " VALUES(";
        } else if (TTable.Stmts.sType.compareTo("VIEW") == 0) {
            TTable.Stmts.bInsert = false;
            insert               = "";
        }

        if (TTable.Stmts.sType.compareTo("VIEW") == 0) {
            /*
            ** Don't know how to retrieve the underlying select so we leave here.
            ** The user will have to edit the rest of the create statement.
            */
            TTable.Stmts.bTransfer    = false;
            TTable.Stmts.bCreate      = true;
            TTable.Stmts.bDelete      = false;
            TTable.Stmts.bDrop        = true;
            TTable.Stmts.bCreateIndex = false;
            TTable.Stmts.bDropIndex   = false;
            TTable.Stmts.bInsert      = false;
            TTable.Stmts.bAlter       = false;

            return;
        }

        ImportedKeys = null;

        try {
            ImportedKeys =
                meta.getImportedKeys(TTable.Stmts.sDatabaseToConvert,
                                     TTable.Stmts.sSchema,
                                     TTable.Stmts.sSourceTable);
        } catch (SQLException e) {
            ImportedKeys = null;
        }

        try {
            if (ImportedKeys != null) {
                while (ImportedKeys.next()) {
                    importedkeys = true;

                    if (!ImportedKeys.getString(12).equals(ConstraintName)) {
                        if (!ConstraintName.equals("")) {
                            alterCreate +=
                                Dest.helper
                                    .formatIdentifier(columnName
                                        .substring(0, columnName
                                            .length() - 1)) + ") REFERENCES "
                                                            + Dest.helper
                                                                .formatName(RefTableName);

                            if (foreignKeyName.length() > 0) {
                                alterCreate +=
                                    " ("
                                    + Dest.helper.formatIdentifier(
                                        foreignKeyName.substring(
                                            0, foreignKeyName.length()
                                            - 1)) + ")";
                            }

                            alterCreate += ";";
                            alterDrop =
                                alterDrop.substring(0, alterDrop.length() - 1)
                                + ";";
                            foreignKeyName = "";
                            columnName     = "";
                        }

                        RefTableName   = ImportedKeys.getString(3);
                        ConstraintName = ImportedKeys.getString(12);
                        alterCreate +=
                            "ALTER TABLE "
                            + Dest.helper.formatName(TTable.Stmts.sDestTable)
                            + " ADD CONSTRAINT ";

                        if ((TTable.Stmts.bFKForced)
                                && (!ConstraintName.startsWith("FK_"))) {
                            alterCreate +=
                                Dest.helper.formatIdentifier(
                                    "FK_" + ConstraintName) + " ";
                        } else {
                            alterCreate +=
                                Dest.helper.formatIdentifier(ConstraintName)
                                + " ";
                        }

                        alterCreate += "FOREIGN KEY (";
                        alterDrop +=
                            "ALTER TABLE "
                            + Dest.helper.formatName(TTable.Stmts.sDestTable)
                            + " DROP CONSTRAINT ";

                        if ((TTable.Stmts.bFKForced)
                                && (!ConstraintName.startsWith("FK_"))) {
                            alterDrop +=
                                Dest.helper.formatIdentifier(
                                    "FK_" + ConstraintName) + " ";
                        } else {
                            alterDrop +=
                                Dest.helper.formatIdentifier(ConstraintName)
                                + " ";
                        }
                    }

                    columnName     += ImportedKeys.getString(8) + ",";
                    foreignKeyName += ImportedKeys.getString(4) + ",";
                }

                ImportedKeys.close();
            }

            if (importedkeys) {
                alterCreate += columnName.substring(0, columnName.length() - 1)
                               + ") REFERENCES "
                               + Dest.helper.formatName(RefTableName);

                if (foreignKeyName.length() > 0) {
                    alterCreate +=
                        " ("
                        + Dest.helper.formatIdentifier(
                            foreignKeyName.substring(
                                0, foreignKeyName.length() - 1)) + ")";
                }

                alterCreate += ";";
                alterDrop = alterDrop.substring(0, alterDrop.length() - 1)
                            + ";";
                TTable.Stmts.sDestDrop = alterDrop + TTable.Stmts.sDestDrop;
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        boolean primarykeys           = false;
        String  PrimaryKeysConstraint = new String();

        PrimaryKeysConstraint = "";

        ResultSet PrimaryKeys = null;

        try {
            PrimaryKeys = meta.getPrimaryKeys(TTable.Stmts.sDatabaseToConvert,
                                              TTable.Stmts.sSchema,
                                              TTable.Stmts.sSourceTable);
        } catch (SQLException e) {
            PrimaryKeys = null;
        }

        try {
            if (PrimaryKeys != null) {
                while (PrimaryKeys.next()) {
                    if (primarykeys) {
                        PrimaryKeysConstraint += ", ";
                    } else {
                        if (PrimaryKeys.getString(6) != null) {
                            PrimaryKeysConstraint =
                                " CONSTRAINT "
                                + Dest.helper.formatIdentifier(
                                    PrimaryKeys.getString(6));
                        }

                        PrimaryKeysConstraint += " PRIMARY KEY (";
                    }

                    PrimaryKeysConstraint +=
                        Dest.helper.formatIdentifier(PrimaryKeys.getString(4));
                    primarykeys = true;
                }

                PrimaryKeys.close();

                if (primarykeys) {
                    PrimaryKeysConstraint += ") ";
                }
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        boolean   indices     = false;
        ResultSet Indices     = null;
        String    IndiceName  = new String("");
        String    CreateIndex = new String("");
        String    DropIndex   = new String("");

        try {
            Indices = meta.getIndexInfo(TTable.Stmts.sDatabaseToConvert,
                                        TTable.Stmts.sSchema,
                                        TTable.Stmts.sSourceTable, false,
                                        false);
        } catch (SQLException e) {
            Indices = null;
        }

        try {
            if (Indices != null) {
                while (Indices.next()) {
                    String tmpIndexName = null;

                    try {
                        tmpIndexName = Indices.getString(6);
                    } catch (SQLException e) {
                        tmpIndexName = null;
                    }

                    if (tmpIndexName == null) {
                        continue;
                    }

                    if (!tmpIndexName.equals(IndiceName)) {
                        if (!IndiceName.equals("")) {
                            CreateIndex =
                                CreateIndex.substring(
                                    0, CreateIndex.length() - 1) + ");";
                            DropIndex += ";";
                        }

                        IndiceName = tmpIndexName;
                        DropIndex  += "DROP INDEX ";

                        if ((TTable.Stmts.bIdxForced)
                                && (!IndiceName.startsWith("Idx_"))) {
                            DropIndex += Dest.helper.formatIdentifier("Idx_"
                                    + IndiceName);
                        } else {
                            DropIndex +=
                                Dest.helper.formatIdentifier(IndiceName);
                        }

                        CreateIndex += "CREATE ";

                        if (!Indices.getBoolean(4)) {
                            CreateIndex += "UNIQUE ";
                        }

                        CreateIndex += "INDEX ";

                        if ((TTable.Stmts.bIdxForced)
                                && (!IndiceName.startsWith("Idx_"))) {
                            CreateIndex += Dest.helper.formatIdentifier("Idx_"
                                    + IndiceName);
                        } else {
                            CreateIndex +=
                                Dest.helper.formatIdentifier(IndiceName);
                        }

                        CreateIndex +=
                            " ON "
                            + Dest.helper.formatName(TTable.Stmts.sDestTable)
                            + "(";
                    }

                    CreateIndex +=
                        Dest.helper.formatIdentifier(Indices.getString(9))
                        + ",";
                    indices = true;
                }

                Indices.close();

                if (indices) {
                    CreateIndex =
                        CreateIndex.substring(0, CreateIndex.length() - 1)
                        + ");";
                    DropIndex += ";";
                }
            }
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        Vector v = new Vector();

        tracer.trace("Reading source columns for table "
                     + TTable.Stmts.sSourceTable);

        ResultSet         col            = null;
        int               colnum         = 1;
        Statement         stmt           = null;
        ResultSet         select_rs      = null;
        ResultSetMetaData select_rsmdata = null;

        try {
            stmt           = conn.createStatement();
            select_rs      = stmt.executeQuery(TTable.Stmts.sSourceSelect);
            select_rsmdata = select_rs.getMetaData();
            col = meta.getColumns(TTable.Stmts.sDatabaseToConvert,
                                  TTable.Stmts.sSchema,
                                  TTable.Stmts.sSourceTable, null);
        } catch (SQLException eSchema) {

            // fredt - second try with null schema
            if (TTable.Stmts.sSchema.equals("")) {
                try {
                    col = meta.getColumns(TTable.Stmts.sDatabaseToConvert,
                                          null, TTable.Stmts.sSourceTable,
                                          null);
                } catch (SQLException eSchema1) {}
            }
        }

        try {
            while (col.next()) {
                String name = Dest.helper.formatIdentifier(col.getString(4));
                int    type        = col.getShort(5);
                String source      = col.getString(6);
                int    column_size = col.getInt(7);
                String DefaultVal  = col.getString(13);
                boolean rsmdata_NoNulls =
                    (select_rsmdata.isNullable(colnum)
                     == java.sql.DatabaseMetaData.columnNoNulls);
                boolean rsmdata_isAutoIncrement = false;

                try {
                    rsmdata_isAutoIncrement =
                        select_rsmdata.isAutoIncrement(colnum);
                } catch (SQLException e) {
                    rsmdata_isAutoIncrement = false;
                }

                int rsmdata_precision = select_rsmdata.getPrecision(colnum);
                int rsmdata_scale     = select_rsmdata.getScale(colnum);

                type = helper.convertFromType(type);
                type = Dest.helper.convertToType(type);

                Integer inttype  = new Integer(type);
                String  datatype = (String) TTable.hTypes.get(inttype);

                if (datatype == null) {
                    datatype = source;

                    tracer.trace("No mapping for type: " + name + " type: "
                                 + type + " source: " + source);
                }

                if (type == Types.NUMERIC) {
                    datatype += "(" + Integer.toString(rsmdata_precision);

                    if (rsmdata_scale > 0) {
                        datatype += "," + Integer.toString(rsmdata_scale);
                    }

                    datatype += ")";
                } else if (type == Types.CHAR) {
                    datatype += "(" + Integer.toString(column_size) + ")";
                } else if (rsmdata_isAutoIncrement) {
                    datatype = "SERIAL";
                }

                if (DefaultVal != null) {
                    if (type == Types.CHAR || type == Types.VARCHAR
                            || type == Types.LONGVARCHAR
                            || type == Types.BINARY || type == Types.DATE
                            || type == Types.TIME || type == Types.TIMESTAMP) {
                        DefaultVal = "\'" + DefaultVal + "\'";
                    }

                    datatype += " DEFAULT " + DefaultVal;
                }

                if (rsmdata_NoNulls) {
                    datatype += " NOT NULL ";
                }

                v.addElement(inttype);

                datatype = helper.fixupColumnDefRead(TTable, select_rsmdata,
                                                     datatype, col, colnum);
                datatype = Dest.helper.fixupColumnDefWrite(TTable,
                        select_rsmdata, datatype, col, colnum);
                create += name + " " + datatype + ",";
                insert += "?,";

                colnum++;
            }

            select_rs.close();
            stmt.close();
            col.close();
        } catch (SQLException e) {
            throw new DataAccessPointException(e.getMessage());
        }

        if (primarykeys) {
            create += PrimaryKeysConstraint + ",";
        }

        TTable.Stmts.sDestCreate = create.substring(0, create.length() - 1)
                                   + ")";
        TTable.Stmts.sDestInsert = insert.substring(0, insert.length() - 1)
                                   + ")";

        if (importedkeys) {
            TTable.Stmts.bAlter     = true;
            TTable.Stmts.sDestAlter = alterCreate;
        } else {
            TTable.Stmts.bAlter = false;
        }

        if (indices) {
            TTable.Stmts.bCreateIndex     = true;
            TTable.Stmts.bDropIndex       = true;
            TTable.Stmts.sDestCreateIndex = CreateIndex;
            TTable.Stmts.sDestDropIndex   = DropIndex;
        } else {
            TTable.Stmts.bCreateIndex = false;
            TTable.Stmts.bDropIndex   = false;
        }

        //iColumnType = new int[v.size()];
        //for (int j = 0; j < v.size(); j++) {
        //    iColumnType[j] = ((Integer) v.elementAt(j)).intValue();
        //}
    }

    void close() throws DataAccessPointException {

        if (srcStatement != null) {
            try {
                srcStatement.close();
            } catch (SQLException e) {}

            srcStatement = null;
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {}

            conn = null;
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
    private void transferRow(TransferResultSet r, PreparedStatement p,
                             int len,
                             int[] types)
                             throws DataAccessPointException, SQLException {

        for (int i = 1; i <= len; i++) {
            int    t = types[i];
            Object o = r.getObject(i);

            if (o == null) {
                if (p != null) {
                    p.setNull(i, t);
                }
            } else {
                o = helper.convertColumnValue(o, i, t);

                p.setObject(i, o);
            }
        }

        if (p != null) {
            p.execute();
        }
    }

    /**
     * @return Returns the meta.
     */
    public DatabaseMetaData getMeta() {
        return meta;
    }

    /**
     * @return Returns the conn.
     */
    public Connection getConn() {
        return conn;
    }
}
