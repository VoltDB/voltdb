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
import java.util.Vector;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Label;
import java.awt.Panel;
import java.awt.ScrollPane;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.TextEvent;
import java.awt.event.TextListener;

/**
 * Class declaration
 *
 *
 * @author ulrivo@users
 * @version 1.0.0
 */

// an entry panel to input/edit a record of a sql table
// ZaurusTableForm is constructed with a tableName and a connection
public class ZaurusTableForm extends ScrollPane
implements TextListener, ItemListener, ActionListener {

    // connection to database - brought via the constructor
    Connection       cConn;
    DatabaseMetaData dbmeta;

    // the name of table for the form
    String tableName;

    // array holding the components (TextField or Choice) in the GUI
    ZaurusComponent[] komponente;

    // the columns of the table
    String[] columns;

    // and their types
    short[] columnTypes;

    // the names of the primary keys of the table
    String[] primaryKeys;

    // the position of the primary keys in the table i. e. the column index starting from 0
    int[] pkColIndex;

    // the names of the imported/foreign keys of the table
    // first dimension is running through the constraints, second dim through the keys of one constraint
    String[][] importedKeys;

    // the position of the imported keys in the table i. e. the column index starting from 0
    int[][] imColIndex;

    // the names of the tables and columns which are the reference for the imported keys
    String[]   refTables;
    String[][] refColumns;

    // the position of the reference keys in the reference table i. e. the column index starting from 0
    int[][] refColIndex;

    // an array holding array of primary keys values matching the search condition
    // first dimension through the results, second dimension running through the primary keys
    Object[][] resultRowPKs;

    // there is an explicit count because a delete may shrink the result rows
    int numberOfResult;

    // prepared statement to fetch the required rows
    PreparedStatement pStmt;

    // pointer into the resultRowPKs
    int aktRowNr;

    public ZaurusTableForm(String name, Connection con) {

        super();

        tableName = name;
        cConn     = con;

        this.fetchColumns();
        this.fetchPrimaryKeys();

        // System.out.print("primaryKeys: ");
        //  for (int i=0; i<primaryKeys.length;i++) {
        // System.out.print(primaryKeys[i]+", ");
        //  } // end of for (int i=0; i<primaryKeys.length;i++)
        //  System.out.println();
        this.fetchImportedKeys();
        this.initGUI();
    }

    // cancel the change/update of a row - show the row again
    public void cancelChanges() {
        this.showAktRow();
    }

    // delete current row, answer special action codes, see comment below
    public int deleteRow() {

        // build the delete string
        String deleteString = "DELETE FROM " + tableName
                              + this.generatePKWhere();
        PreparedStatement ps = null;

        // System.out.println("delete string "+deleteString);
        try {

            // fill the question marks
            ps = cConn.prepareStatement(deleteString);

            ps.clearParameters();

            int i;

            for (int j = 0; j < primaryKeys.length; j++) {
                ps.setObject(j + 1, resultRowPKs[aktRowNr][j]);
            }    // end of for (int i=0; i<primaryKeys.length; i++)

            ps.executeUpdate();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());

            return 0;
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {}
        }

        // delete the corresponding primary key values from resultRowPKs
        numberOfResult--;

        for (int i = aktRowNr; i < numberOfResult; i++) {
            for (int j = 0; j < primaryKeys.length; j++) {
                resultRowPKs[i][j] = resultRowPKs[i + 1][j];
            }
        }

        // there are the following outcomes after deleting aktRowNr:
        /*
                                           A B C D E F
        no rows left                   J N N N N N
        one row left                   - J N J N N
        deleted row was the last row   - J J N N N
        deleted row was the pre-last   - - - - J N

            first                          D X + D + *
             .                               D X X D D
             .                                 D   X +
            last                                     X

            new numberOfResult             0 1 2 1 2 2
            old aktRowNr                     0 1 2 0 1 0

        D - deleted row
            X - any one row
            + - one or more rows
        * - zero or more rows

        */

        // A. return to the search panel and tell 'last row deleted' on the status line
        // B. show the previous row and disable previous button
        // C. show the previous row as akt row
        // D. show akt row and disable next button
        // E. show akt row and disable next button
        // F. show akt row
        // these actions reduce to the following actions for ZaurusEditor:
        // 1. show search panel
        // 2. disable previous button
        // 3. disable next button
        // 4. do nothing
        // and 1,2,3,4 are the possible return codes
        int actionCode;

        if (numberOfResult == 0) {

            // case A
            actionCode = 1;

            ZaurusEditor.printStatus("Last row was deleted.");

            return actionCode;
        } else if (numberOfResult == aktRowNr) {

            // B or C
            // new aktRow is previous row
            aktRowNr--;

            if (aktRowNr == 0) {

                // B
                actionCode = 2;
            } else {

                // C
                actionCode = 4;
            }    // end of if (aktRowNr == 0)
        } else {

            // D, E, F
            if (numberOfResult >= 2 && aktRowNr < numberOfResult - 1) {

                // F
                actionCode = 4;
            } else {
                actionCode = 3;
            }    // end of else
        }

        this.showAktRow();
        ZaurusEditor.printStatus("Row was deleted.");

        return actionCode;
    }

    // answer a String containing a String list of primary keys i. e. "pk1, pk2, pk3"
    public String getPrimaryKeysString() {

        String result = "";

        for (int i = 0; i < primaryKeys.length; i++) {
            if (result != "") {
                result += ", ";
            }

            result += primaryKeys[i];
        }    // end of for (int i=0; i<primaryKeys.length; i++)

        return result;
    }

    // open the panel to insert a new row into the table
    public void insertNewRow() {

        // reset all fields
        for (int i = 0; i < komponente.length; i++) {
            komponente[i].clearContent();
        }    // end of for (int i=0; i<komponente.length; i++)

        // reset the field for the primary keys
        for (int i = 0; i < primaryKeys.length; i++) {
            komponente[pkColIndex[i]].setEditable(true);
        }

        ZaurusEditor.printStatus("enter a new row for table " + tableName);
    }

    // show next row
    // answer true, if there is after the next row another row
    public boolean nextRow() {

        if (aktRowNr + 1 == numberOfResult) {
            return false;
        }

        aktRowNr++;

        this.showAktRow();

        return (aktRowNr + 1 < numberOfResult);
    }

    // show prev row
    // answer true, if there is previous the previous row another row
    public boolean prevRow() {

        if (aktRowNr == 0) {
            return false;
        }

        aktRowNr--;

        this.showAktRow();

        return (aktRowNr > 0);
    }

    // save all changes which are be made in the textfelder to the database
    // answer true, if the update succeeds
    public boolean saveChanges() {

        // the initial settings of the textfields counts with one
        // so a real change by the user needs as many changes as there are columns
        // System.out.print("Anderungen in den Feldern: ");
        // there are changes to the database
        // memorize all columns which have been changed
        int[] changedColumns = new int[columns.length];
        int   countChanged   = 0;

        // build the update string
        String updateString = "";

        for (int i = 0; i < columns.length; i++) {
            if (komponente[i].hasChanged()) {
                if (updateString != "") {
                    updateString += ", ";
                }

                updateString                   += columns[i] + "=?";
                changedColumns[countChanged++] = i;
            }
        }    // end of for (int i=0; i<columns.length; i++)

        if (countChanged > 0) {
            updateString = "UPDATE " + tableName + " SET " + updateString
                           + this.generatePKWhere();

            PreparedStatement ps = null;

            // System.out.println("update "+updateString);
            try {

                // fill the question marks
                ps = cConn.prepareStatement(updateString);

                ps.clearParameters();

                int i;

                for (i = 0; i < countChanged; i++) {
                    ps.setObject(i + 1,
                                 komponente[changedColumns[i]].getContent());

                    // System.out.print(" changed feld "+komponente[changedColumns[i]].getContent());
                }    // end of for (int i=0; i<countChanged; i++)

                // System.out.println();
                for (int j = 0; j < primaryKeys.length; j++) {
                    ps.setObject(i + j + 1, resultRowPKs[aktRowNr][j]);
                }    // end of for (int i=0; i<primaryKeys.length; i++)

                ps.executeUpdate();
                ZaurusEditor.printStatus("changed row was saved to table "
                                         + tableName);

                return true;
            } catch (SQLException e) {
                ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());

                return false;
            } finally {
                try {
                    if (ps != null) {
                        ps.close();
                    }
                } catch (SQLException e) {}
            }
        } else {

            //              System.out.println("no changes");
            return true;
        }            // end of if (changed)
    }

    // save a new row
    // answer true, if saving succeeds
    public boolean saveNewRow() {

        // check the fields of the primary keys whether one is empty
        boolean           onePKempty = false;
        int               tmp;
        PreparedStatement ps = null;

        for (tmp = 0; tmp < primaryKeys.length; tmp++) {
            if (komponente[pkColIndex[tmp]].getContent().equals("")) {
                onePKempty = true;

                break;
            }
        }

        if (onePKempty) {
            komponente[pkColIndex[tmp]].requestFocus();
            ZaurusEditor.printStatus("no value for primary key "
                                     + primaryKeys[tmp]);

            return false;
        }    // end of if (onePKempty)

        // build the insert string
        String insertString = "INSERT INTO " + tableName + " VALUES(";

        for (int j = 0; j < columns.length; j++) {
            if (j > 0) {
                insertString += ", ";
            }

            insertString += "?";
        }    // end of for (int i=0; i<columns.length; i++)

        insertString += ")";

        // System.out.println("insert string "+insertString);
        try {

            // fill the question marks
            ps = cConn.prepareStatement(insertString);

            ps.clearParameters();

            int i;

            for (i = 0; i < columns.length; i++) {
                ps.setObject(i + 1, komponente[i].getContent());
            }

            ps.executeUpdate();
            ZaurusEditor.printStatus("new row was saved to table "
                                     + tableName);

            return true;
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());

            return false;
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {}
        }
    }

    // read all primary key values into resultRowPKs for the rows which meet the search condition i. e.
    // which contains the search words
    // answer the number of found rows, -1 if there is an SQL exception
    public int searchRows(String[] words, boolean allWords,
                          boolean ignoreCase, boolean noMatchWhole) {

        // System.out.print("search in " + tableName + " for: ");
        //  for (int i=0; i < words.length; i++) {
        //      System.out.print(words[i]+", ");
        //  }
        // System.out.println("allWords = "+allWords+", ignoreCase = "+ignoreCase+", noMatchWhole= "+noMatchWhole);
        String where = this.generateWhere(words, allWords, ignoreCase,
                                          noMatchWhole);
        Vector    temp = new Vector(20);
        Statement stmt = null;

        try {
            stmt = cConn.createStatement();

            ResultSet rs = stmt.executeQuery("SELECT "
                                             + this.getPrimaryKeysString()
                                             + " FROM " + tableName + where);

            while (rs.next()) {
                Object[] pkValues = new Object[primaryKeys.length];

                for (int i = 0; i < primaryKeys.length; i++) {
                    pkValues[i] = rs.getObject(pkColIndex[i] + 1);
                }    // end of for (int i=0; i<primaryKeys.length; i++)

                temp.addElement(pkValues);
            }

            rs.close();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());

            return -1;
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {}
        }

        resultRowPKs   = new Object[temp.size()][primaryKeys.length];
        numberOfResult = temp.size();

        for (int i = 0; i < primaryKeys.length; i++) {
            for (int j = 0; j < temp.size(); j++) {
                resultRowPKs[j][i] = ((Object[]) temp.elementAt(j))[i];
            }    // end of for (int j=0; j<temp.size(); j++)
        }        // end of for (int i=0; i<primaryKeys.length; i++)

        // prepare statement for fetching the result rows for later use
        String stmtString = "SELECT * FROM " + tableName;

        try {
            pStmt = cConn.prepareStatement(stmtString
                                           + this.generatePKWhere());
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }    // end of try-catch

        // System.out.println("prepared statement: "+stmtString);
        if (numberOfResult > 0) {
            this.disablePKFields();

            aktRowNr = 0;

            this.showAktRow();
        }    // end of if (numberOfResult > 0)

        // System.out.println("number of rows: "+numberOfResult);
        return numberOfResult;
    }

    public void actionPerformed(ActionEvent e) {}

    public void textValueChanged(TextEvent e) {

        for (int i = 0; i < columns.length; i++) {
            if (komponente[i] == e.getSource()) {
                komponente[i].setChanged();

                break;
            }
        }
    }

    public void itemStateChanged(ItemEvent e) {

        for (int i = 0; i < columns.length; i++) {
            if (komponente[i] == e.getSource()) {
                komponente[i].setChanged();

                break;
            }
        }
    }

    // ******************************************************
    // private methods
    // ******************************************************
    // set all fields for primary keys to not editable
    private void disablePKFields() {

        for (int i = 0; i < primaryKeys.length; i++) {
            komponente[pkColIndex[i]].setEditable(false);
        }    // end of for (int i=0; i<columns.length; i++)
    }

    // fetch all values from a table and a column
    // fill the ZaurusChoice zc with the row values for the Choice
    // and the column values as values
    private void fillZChoice(ZaurusChoice zc, String tab, String col) {

        Statement stmt = null;

        try {
            if (cConn == null) {
                return;
            }

            stmt = cConn.createStatement();

            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tab
                                             + " ORDER BY " + col);
            ResultSetMetaData rsmd            = rs.getMetaData();
            int               numberOfColumns = rsmd.getColumnCount();
            int               colIndex        = rs.findColumn(col);

            while (rs.next()) {
                String tmp = "";

                for (int i = 1; i <= numberOfColumns; i++) {
                    if (i > 1) {
                        tmp += "; ";
                    }

                    tmp += rs.getString(i);
                }    // end of for (int i=1; i<=numberOfColumns; i++)

                zc.add(tmp, rs.getString(colIndex));
            }

            rs.close();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {}
        }
    }

    // fetch all column names
    private void fetchColumns() {

        Vector temp     = new Vector(20);
        Vector tempType = new Vector(20);

        try {
            if (cConn == null) {
                return;
            }

            if (dbmeta == null) {
                dbmeta = cConn.getMetaData();
            }

            ResultSet colList = dbmeta.getColumns(null, null, tableName, "%");

            while (colList.next()) {
                temp.addElement(colList.getString("COLUMN_NAME"));
                tempType.addElement(new Short(colList.getShort("DATA_TYPE")));
            }

            colList.close();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());
        }

        columns = new String[temp.size()];

        temp.copyInto(columns);

        columnTypes = new short[temp.size()];

        for (int i = 0; i < columnTypes.length; i++) {
            columnTypes[i] = ((Short) tempType.elementAt(i)).shortValue();
        }
    }

    // fetch the imported keys i.e. columns which reference to foreign keys in other tables
    private void fetchImportedKeys() {

        Vector imKeys      = new Vector(20);
        Vector imKeyNames  = null;
        Vector refTabs     = new Vector(20);
        Vector refCols     = new Vector(20);
        Vector refColNames = null;

        try {
            if (cConn == null) {
                return;
            }

            if (dbmeta == null) {
                dbmeta = cConn.getMetaData();
            }

            ResultSet colList = dbmeta.getImportedKeys(null, null, tableName);
            String    pkTable, pkColumn, fkColumn;
            int       keySeq;

            while (colList.next()) {
                pkTable  = colList.getString("PKTABLE_NAME");
                pkColumn = colList.getString("PKCOLUMN_NAME");
                fkColumn = colList.getString("FKCOLUMN_NAME");
                keySeq   = colList.getInt("KEY_SEQ");

                if (keySeq == 1) {
                    if (imKeyNames != null) {
                        imKeys.addElement(imKeyNames);
                        refCols.addElement(refColNames);
                    }    // end of if (exKeyNames != null)

                    imKeyNames  = new Vector(20);
                    refColNames = new Vector(20);

                    refTabs.addElement(pkTable);
                }        // end of if (keySeq == 1)

                imKeyNames.addElement(fkColumn);
                refColNames.addElement(pkColumn);
            }

            if (imKeyNames != null) {
                imKeys.addElement(imKeyNames);
                refCols.addElement(refColNames);
            }            // end of if (exKeyNames != null)

            colList.close();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());
        }

        // System.out.println("Imported Keys of "+tableName);
        int numberOfConstraints = imKeys.size();

        importedKeys = new String[numberOfConstraints][];
        imColIndex   = new int[numberOfConstraints][];
        refTables    = new String[numberOfConstraints];
        refColumns   = new String[numberOfConstraints][];
        refColIndex  = new int[numberOfConstraints][];

        for (int i = 0; i < numberOfConstraints; i++) {
            Vector keys         = (Vector) imKeys.elementAt(i);
            Vector cols         = (Vector) refCols.elementAt(i);
            int    numberOfKeys = keys.size();

            importedKeys[i] = new String[numberOfKeys];
            imColIndex[i]   = new int[numberOfKeys];
            refColumns[i]   = new String[numberOfKeys];
            refColIndex[i]  = new int[numberOfKeys];
            refTables[i]    = (String) refTabs.elementAt(i);

            // System.out.println("reference table "+refTables[i]);
            for (int j = 0; j < numberOfKeys; j++) {
                importedKeys[i][j] = (String) keys.elementAt(j);
                imColIndex[i][j]   = this.getColIndex(importedKeys[i][j]);
                refColumns[i][j]   = (String) cols.elementAt(j);
                refColIndex[i][j] = this.getColIndex(refColumns[i][j],
                                                     refTables[i]);

                // System.out.println("   importedKeys "+importedKeys[i][j]+"(Index: "+imColIndex[i][j]+") refColumns "+refColumns[i][j]+"(Index: "+refColIndex[i][j]+")");
            }    // end of for (int j=0; j<numberOfKeys; j++)
        }
    }

    private void fetchPrimaryKeys() {

        Vector temp = new Vector(20);

        try {
            if (cConn == null) {
                return;
            }

            if (dbmeta == null) {
                dbmeta = cConn.getMetaData();
            }

            ResultSet colList = dbmeta.getPrimaryKeys(null, null, tableName);

            while (colList.next()) {
                temp.addElement(colList.getString("COLUMN_NAME"));
            }

            colList.close();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());
        }

        primaryKeys = new String[temp.size()];

        temp.copyInto(primaryKeys);

        pkColIndex = new int[primaryKeys.length];

        for (int i = 0; i < primaryKeys.length; i++) {
            pkColIndex[i] = this.getColIndex(primaryKeys[i]);
        }    // end of for (int i=0; i<primaryKeys.length; i++)
    }

    private String generatePKWhere() {

        String stmtString = " WHERE ";

        for (int i = 0; i < primaryKeys.length; i++) {
            if (i > 0) {
                stmtString += " AND ";
            }

            stmtString += primaryKeys[i] + "=?";
        }    // end of for (int i=0; i<primaryKeys.length; i++)

        return stmtString;
    }

    // generate the Where-condition for the words
    private String generateWhere(String[] words, boolean allWords,
                                 boolean ignoreCase, boolean noMatchWhole) {

        String result = "";

        // if all words must match use AND between the different conditions
        String join;

        if (allWords) {
            join = " AND ";
        } else {
            join = " OR ";
        }    // end of else

        for (int wordInd = 0; wordInd < words.length; wordInd++) {
            String oneCondition = "";

            for (int col = 0; col < columns.length; col++) {
                if (oneCondition != "") {
                    oneCondition += " OR ";
                }

                if (ignoreCase) {
                    if (noMatchWhole) {
                        oneCondition += "LOWER(" + columns[col] + ") LIKE '%"
                                        + words[wordInd].toLowerCase() + "%'";
                    } else {
                        oneCondition += "LOWER(" + columns[col] + ") LIKE '"
                                        + words[wordInd].toLowerCase() + "'";
                    }
                } else {
                    if (noMatchWhole) {
                        oneCondition += columns[col] + " LIKE '%"
                                        + words[wordInd] + "%'";
                    } else {
                        oneCondition += columns[col] + " LIKE '"
                                        + words[wordInd] + "'";
                    }
                }
            }

            if (result != "") {
                result += join;
            }

            result += "(" + oneCondition + ")";
        }

        if (result != "") {
            result = " WHERE " + result;
        }    // end of if (result != "")

        // System.out.println("result: "+result);
        return result;
    }

    // answer the index of the column named name in the actual table
    private int getColIndex(String name) {

        for (int i = 0; i < columns.length; i++) {
            if (name.equals(columns[i])) {
                return i;
            }    // end of if (name.equals(columns[i]))
        }        // end of for (int i=0; i<columns.length; i++)

        return -1;
    }

    // answer the index of the column named colName in the table tabName
    private int getColIndex(String colName, String tabName) {

        int ordPos = 0;

        try {
            if (cConn == null) {
                return -1;
            }

            if (dbmeta == null) {
                dbmeta = cConn.getMetaData();
            }

            ResultSet colList = dbmeta.getColumns(null, null, tabName,
                                                  colName);

            colList.next();

            ordPos = colList.getInt("ORDINAL_POSITION");

            colList.close();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }

        return ordPos - 1;
    }

    // answer the index of the constraint for the column index
    // answer -1, if the column is not part of any constraint
    private int getConstraintIndex(int colIndex) {

        for (int i = 0; i < imColIndex.length; i++) {
            for (int j = 0; j < imColIndex[i].length; j++) {
                if (colIndex == imColIndex[i][j]) {
                    return i;
                }    // end of if (col == imColIndex[i][j])
            }        // end of for (int j=0; j<imColIndex[i].length; j++)
        }            // end of for (int i=0; i<imColIndex.length; i++)

        return -1;
    }

    private void initGUI() {

        Panel pEntry = new Panel();

        pEntry.setLayout(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();

        c.fill       = GridBagConstraints.HORIZONTAL;
        c.insets     = new Insets(3, 3, 3, 3);
        c.gridwidth  = 1;
        c.gridheight = 1;
        c.weightx    = c.weighty = 1;
        c.anchor     = GridBagConstraints.WEST;
        komponente   = new ZaurusComponent[columns.length];

        for (int i = 0; i < columns.length; i++) {
            c.gridy = i;
            c.gridx = 0;

            pEntry.add(new Label((String) columns[i]), c);

            c.gridx = 1;

            int constraint = this.getConstraintIndex(i);

            if (constraint >= 0 && imColIndex[constraint].length == 1) {

                // we use ony foreign keys with one index
                ZaurusChoice tmp = new ZaurusChoice();

                this.fillZChoice(tmp, refTables[constraint],
                                 refColumns[constraint][0]);
                tmp.addItemListener(this);

                komponente[i] = tmp;

                pEntry.add(tmp, c);
            } else if (columnTypes[i] == java.sql.Types.DATE) {

                //              System.out.println("hier gibt es eine Date-Spalte namens "+columns[i]);
                ZaurusTextField tmp = new ZaurusTextField(8);

                tmp.addTextListener(this);
                pEntry.add(tmp, c);

                komponente[i] = tmp;
            } else {
                ZaurusTextField tmp = new ZaurusTextField(5);

                tmp.addTextListener(this);
                pEntry.add(tmp, c);

                komponente[i] = tmp;
            }

            komponente[i].setEditable(true);
        }

        this.add(pEntry);
    }

    // get and show the values of the actual row in the GUI
    private void showAktRow() {

        try {
            pStmt.clearParameters();

            for (int i = 0; i < primaryKeys.length; i++) {
                pStmt.setObject(i + 1, resultRowPKs[aktRowNr][i]);
            }    // end of for (int i=0; i<primaryKeys.length; i++)

            ResultSet rs = pStmt.executeQuery();

            rs.next();

            for (int i = 0; i < columns.length; i++) {
                komponente[i].setContent(rs.getString(i + 1));
            }    // end of for (int i=0; i<primaryKeys.length; i++)

            rs.close();
        } catch (SQLException e) {
            ZaurusEditor.printStatus("SQL Exception: " + e.getMessage());
        }        // end of try-catch

        for (int i = 0; i < columns.length; i++) {
            komponente[i].clearChanges();
        }
    }
}
