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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;
import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.CardLayout;
import java.awt.Checkbox;
import java.awt.CheckboxGroup;
import java.awt.Choice;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Label;
import java.awt.Panel;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * <code>ZaurusEditor</code> implements an search/input/edit form to
 * search/view/update/insert table records.
 *
 * @author ulrivo@users
 * @version 1.0
 *
 * <p>
 * Starting on a search panel, one can choose a table from the actual
 * database. The list contains only tables which have a primary key
 * defined.
 *
 * One may provide one or more words which are to be contained in the
 * rows of the table.
 * <p>
 * There are three options in searching these words:
 * <ul>
 * <li>Use search words: all or any
 * <br>
 * Only rows which contain <b>all</b> words in any column will be
 * found. Alternatively, it is sufficient when <b>any</b> of the
 * words is contained in one of the columns.
 * <br>
 * <li>Ignore case :
 * <br>
 * While searching, there is no difference between lower or upper case
 * letters, if this optioned is set to 'yes',
 * <br>
 * <li>Match whole column:
 * <br>
 * It is sufficient that the given word is just a part of any
 * column. For instance, the word 'ring' will be found as part of
 * 'Stringer'. Alternatively, the column content must match the search
 * word completely.
 * </ul><p>
 * After choosing a table, typing one or more search words and
 * choosing some search options, the button <b>Search Rows</b> initiates
 * the search through the table.
 * <br>Alternatively, the button <b>New Row</b> will open an empty input
 * panel for inserting a new row for the  choosen table.<br>In both cases,
 * a table specific panel with an entry field per column is shown.<br>
 *
 */
public class ZaurusEditor extends Panel implements ActionListener {

    // class variables
    // status line
    static TextField tStatus;

    // instance variables
    // connection to database - brought via the refresh method
    Connection       cConn;
    DatabaseMetaData dbmeta;

    // buttons in Editor
    Button bSearchRow, bNewRow;
    Button bCancel1, bPrev, bNext, bDelete, bNewSearch;
    Button bCancel2, bNewInsert, bNewSearch1;

    // button boxes
    Panel pSearchButs, pEditButs, pInsertButs;

    // the list of used table names
    Vector vHoldTableNames;

    // the list of corresponding table forms
    Vector vHoldForms;

    // the pointer into the vHold.... for the actual table
    int aktHoldNr;

    //  pForm and pButton with CardLayout
    Panel      pForm, pButton;
    CardLayout lForm, lButton;
    /*
      ZaurusEditor holds two card panels:

      1. pForm holds the different forms
      pForm shows initially a search form to select a table and to
      input some search words.
      For every table which should be used in the editor, there is an own
      ZaurusTableForm added to the card panel pForm

      2. pButton holds the different button boxes
      For the search form, there are buttons search row, new row  - all in the panel pSearchButs
      For a table in the editor, there are alternatively:
      a) cancel, prev, next, delete row, new search - for editing a row - in panel pEditButs
      b) cancel, new insert, new search - for inserting a new row - pInsertButs
     */

    // text field with search words
    TextField fSearchWords;

    // the choice of all tables
    Choice cTables;

    // the checkbox group for using any/all search words
    CheckboxGroup gAllWords;

    // the checkbox group for ignoring the case of search words
    CheckboxGroup gIgnoreCase;

    // the checkbox group for  matching the whole column i. e. not using LIKE
    CheckboxGroup gNoMatchWhole;

    // one needs a double press of the delete button to delete a row
    boolean lastButtonDelete;

    /**
     * <code>printStatus</code> prints a text into the status line below the panel.
     *
     * @param text a <code>String</code> value will be shown
     */
    public static void printStatus(String text) {
        tStatus.setText(text);
    }

    /**
     * The class method <code>clearStatus</code> deletes the status line.
     *
     */
    public static void clearStatus() {
        tStatus.setText("");
    }

    /**
     * Constructor declaration
     *
     */
    public ZaurusEditor() {

        super();

        initGUI();
    }

    /**
     *  <code>actionPerformed</code> method is the main entry point
     * which interprets the buttons and initiates the actions.
     *
     * @param e an <code>ActionEvent</code> value is been sent to ZaurusEditor as ActionListener
     *
     * <p>The possible events are:<ul> <li>Buttons on the <b>search
     * panel:</b> <dl><dt>Search Row<dd> Starts the search of rows in
     * the choosen table with the given search words and search
     * options. Without any search words, all rows will be found.<br>
     * If no row meets the criteria, there will be a message in the
     * status line.  <dt>New Row<dd> An empty input panel for the
     * choosen table is given. Any search words are
     * ignored.</dl><li>Buttons on the <b>edit panel:</b><br>Any
     * changes to field values on this panel will be updated to the
     * table when the actual row is left, i. e. when pressing one of
     * the buttons 'Prev', 'Next' or
     * 'Search'. <br><dl><dt>Cancel<dd>Any changes to field contents
     * are canceled and reset to the previous values.<dt>Prev<dd>Show
     * the previous row which meets the search
     * criteria.<dt>Next<dd>Show the next row which meets the search
     * criteria.<dt>Delete<dd>This button has to be clicked twice and
     * the shown row will be deleted from the table<dt>Search<dd>With
     * this button a new search is initiated. Any changes made to
     * field contents are saved</dl><li>Buttons on the <b>insert
     * panel:</b><dl><dt>Cancel Insert<dd>After beginning to fill a
     * new row, the insert may be cancelled. The search panel will be
     * shown next.<dt>New Insert<dd>The new row is inserted into the
     * table and a new empty insert panel is shown.<dt>New
     * Search<dd>The new row is inserted into the table and the search
     * panel is shown again.</ul>
     */
    public void actionPerformed(ActionEvent e) {

        Button button = (Button) e.getSource();

        if (button == bSearchRow) {
            this.resetLastButtonDelete();

            // System.out.println("pressed search");
            aktHoldNr = getChoosenTableIndex();

            // search all rows
            int numberOfRows =
                ((ZaurusTableForm) vHoldForms.elementAt(aktHoldNr))
                    .searchRows(this
                        .getWords(), (gAllWords.getSelectedCheckbox()
                            .getLabel().equals("all")), (gIgnoreCase
                            .getSelectedCheckbox().getLabel()
                            .equals("yes")), (gNoMatchWhole
                            .getSelectedCheckbox().getLabel().equals("no")));
            String tableName = (String) vHoldTableNames.elementAt(aktHoldNr);

            if (numberOfRows > 0) {
                lForm.show(pForm, tableName);
                lButton.show(pButton, "edit");
                bPrev.setEnabled(false);

                // if there is more than one row, enable the next button
                bNext.setEnabled(numberOfRows != 1);
                ZaurusEditor.printStatus("found " + numberOfRows
                                         + " rows in table " + tableName);
            } else if (numberOfRows == 0) {
                ZaurusEditor.printStatus("no rows found in table "
                                         + tableName);

                // numberOfRows could be -1 as well, if an SQL exception was encountered
            }        // end of if (numberOfRows > 0)
        } else if ((button == bNewRow)) {

            // System.out.println("pressed new");
            aktHoldNr = getChoosenTableIndex();

            lForm.show(pForm, (String) vHoldTableNames.elementAt(aktHoldNr));
            lButton.show(pButton, "insert");
            ((ZaurusTableForm) vHoldForms.elementAt(
                aktHoldNr)).insertNewRow();
        } else if (button == bNewInsert) {
            this.resetLastButtonDelete();

            // new search in edit row
            if (((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).saveNewRow()) {

                // System.out.println("pressed new insert");
                ((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).insertNewRow();
            }
        } else if (button == bNewSearch) {
            this.resetLastButtonDelete();

            // new search in edit row
            if (((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).saveChanges()) {

                // System.out.println("pressed new search");
                lForm.show(pForm, "search");
                lButton.show(pButton, "search");
            }
        } else if (button == bNewSearch1) {
            this.resetLastButtonDelete();

            // new search in insert row, succeeds if the saving is successfull
            if (((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).saveNewRow()) {

                // System.out.println("pressed new search");
                lForm.show(pForm, "search");
                lButton.show(pButton, "search");
            }
        } else if ((button == bNext)) {
            this.resetLastButtonDelete();
            ZaurusEditor.clearStatus();

            if (((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).saveChanges()) {
                bPrev.setEnabled(true);

                if (!((ZaurusTableForm) vHoldForms.elementAt(
                        aktHoldNr)).nextRow()) {
                    bNext.setEnabled(false);
                }
            }
        } else if ((button == bPrev)) {
            this.resetLastButtonDelete();
            ZaurusEditor.clearStatus();

            if (((ZaurusTableForm) vHoldForms.elementAt(
                    aktHoldNr)).saveChanges()) {
                bNext.setEnabled(true);

                if (!((ZaurusTableForm) vHoldForms.elementAt(
                        aktHoldNr)).prevRow()) {
                    bPrev.setEnabled(false);
                }
            }
        } else if ((button == bCancel1)) {

            // cancel in edit panel
            this.resetLastButtonDelete();
            ((ZaurusTableForm) vHoldForms.elementAt(
                aktHoldNr)).cancelChanges();
        } else if ((button == bCancel2)) {
            this.resetLastButtonDelete();

            // cancel in insert panel, just show the search panel again
            lForm.show(pForm, "search");
            lButton.show(pButton, "search");
        } else if (button == bDelete) {
            if (lastButtonDelete) {

                // delete and determine follow up actions, see comment in ZaurusTableForm.deleteRow()
                switch (((ZaurusTableForm) vHoldForms.elementAt(
                        aktHoldNr)).deleteRow()) {

                    case 1 :
                        lForm.show(pForm, "search");
                        lButton.show(pButton, "search");
                        break;

                    case 2 :
                        bPrev.setEnabled(false);
                        break;

                    case 3 :
                        bNext.setEnabled(false);
                        break;

                    default :
                        break;
                }    // end of switch (((ZaurusTableForm) vHoldForms.elementAt(aktHoldNr)).deleteRow())

                lastButtonDelete = false;
            } else {
                ZaurusEditor.printStatus(
                    "Press 'Delete' a second time to delete row.");

                lastButtonDelete = true;
            }        // end of if (lastButtonDelete)
        }            // end of if (button == Rest)
    }

    // when tree is refreshed ZaurusEditor.refresh() is called, too

    /**
     *  <code>refresh</code> will read again the meta data of the
     * actual database. This is useful after changes of the table
     * structures for instance creating or dropping tables, or
     * altering tabel. The method will be called if one asks to
     * refresh the tree or if the connection to the database is
     * changed.
     *
     * @param c a <code>Connection</code> is the actual connection to
     * the database
     */
    public void refresh(Connection c) {

        cConn = c;

        if (vHoldForms == null) {
            this.initGUI();

            // System.out.println("initGUI <<<<<<<<<<<<<<<<<<<<<<");
        } else {
            this.resetTableForms();

            // System.out.println("reset >>>>>>>>>>>>>>>>>");
        }
    }

    private void initGUI() {

        // without connection there are no tables
        // vAllTables is a local variable with all table names in the database
        // vHoldTableNames holds the table names which have a ZaurusTableForm
        Vector vAllTables = getAllTables();

        if (vAllTables == null) {
            return;
        }

        // initialize a new list for the table names which have a form in pForm
        vHoldTableNames = new Vector(20);
        vHoldForms      = new Vector(20);

        // this holds the card panel pForm for the forms in the top
        // a card panel pButton below
        // the both card panels form a panel which is centered in this
        // and a status line in the south
        this.setLayout(new BorderLayout(3, 3));

        // >>> the top of this: the entry forms in pForm
        // pFormButs holds in the center the forms card panel pForm and
        // in the south the button card panel pButton
        Panel pFormButs = new Panel();

        pFormButs.setLayout(new BorderLayout(3, 3));

        pForm = new Panel();
        lForm = new CardLayout(2, 2);

        pForm.setLayout(lForm);

        // the search panel containing the list of all tables and
        // the entry fields for search words in the Center
        Panel pEntry = new Panel();

        pEntry.setLayout(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();

        c.fill       = GridBagConstraints.HORIZONTAL;
        c.insets     = new Insets(3, 3, 3, 3);
        c.gridwidth  = 1;
        c.gridheight = 1;
        c.weightx    = c.weighty = 1;
        c.anchor     = GridBagConstraints.WEST;
        c.gridy      = 0;
        c.gridx      = 0;

        pEntry.add(new Label("Search table"), c);

        c.gridx = 1;

        // get all table names and show a drop down list of them in cTables
        cTables = new Choice();

        for (Enumeration e = vAllTables.elements(); e.hasMoreElements(); ) {
            cTables.addItem((String) e.nextElement());
        }

        c.gridwidth = 2;

        pEntry.add(cTables, c);

        c.gridy     = 1;
        c.gridx     = 0;
        c.gridwidth = 1;

        pEntry.add(new Label("Search words"), c);

        c.gridx      = 1;
        c.gridwidth  = 2;
        fSearchWords = new TextField(8);

        pEntry.add(fSearchWords, c);

        // use search words
        c.gridwidth = 1;
        c.gridy     = 2;
        c.gridx     = 0;

        pEntry.add(new Label("Use search words"), c);

        gAllWords = new CheckboxGroup();

        Checkbox[] checkboxes = new Checkbox[2];

        checkboxes[0] = new Checkbox("all", gAllWords, true);
        c.gridx       = 1;

        pEntry.add(checkboxes[0], c);

        checkboxes[1] = new Checkbox("any ", gAllWords, false);
        c.gridx       = 2;

        pEntry.add(checkboxes[1], c);

        // ignore case
        c.gridy = 3;
        c.gridx = 0;

        pEntry.add(new Label("Ignore case"), c);

        gIgnoreCase = new CheckboxGroup();

        Checkbox[] checkboxes1 = new Checkbox[2];

        checkboxes1[0] = new Checkbox("yes", gIgnoreCase, true);
        c.gridx        = 1;

        pEntry.add(checkboxes1[0], c);

        checkboxes1[1] = new Checkbox("no", gIgnoreCase, false);
        c.gridx        = 2;

        pEntry.add(checkboxes1[1], c);

        // Match column exactly
        c.gridy = 4;
        c.gridx = 0;

        pEntry.add(new Label("Match whole col"), c);

        gNoMatchWhole = new CheckboxGroup();

        Checkbox[] checkboxes2 = new Checkbox[2];

        checkboxes2[0] = new Checkbox("no", gNoMatchWhole, true);
        c.gridx        = 1;

        pEntry.add(checkboxes2[0], c);

        checkboxes2[1] = new Checkbox("yes ", gNoMatchWhole, false);
        c.gridx        = 2;

        pEntry.add(checkboxes2[1], c);
        pForm.add("search", pEntry);
        pFormButs.add("Center", pForm);

        // the buttons
        this.initButtons();

        pButton = new Panel();
        lButton = new CardLayout(2, 2);

        pButton.setLayout(lButton);
        pButton.add("search", pSearchButs);
        pButton.add("edit", pEditButs);
        pButton.add("insert", pInsertButs);
        pFormButs.add("South", pButton);
        this.add("Center", pFormButs);

        // >>> the South: status line at the bottom
        Font fFont = new Font("Dialog", Font.PLAIN, 10);

        ZaurusEditor.tStatus = new TextField("");

        ZaurusEditor.tStatus.setEditable(false);
        this.add("South", ZaurusEditor.tStatus);
    }

    // process the buttons events
    // *******************************************************
    // private methods
    // *******************************************************
    // read all table names over the current database connection
    // exclude tables without primary key
    private Vector getAllTables() {

        Vector result = new Vector(20);

        try {
            if (cConn == null) {
                return null;
            }

            dbmeta = cConn.getMetaData();

            String[] tableTypes = { "TABLE" };
            ResultSet allTables = dbmeta.getTables(null, null, null,
                                                   tableTypes);

            while (allTables.next()) {
                String aktTable = allTables.getString("TABLE_NAME");
                ResultSet primKeys = dbmeta.getPrimaryKeys(null, null,
                    aktTable);

                // take only table with a primary key
                if (primKeys.next()) {
                    result.addElement(aktTable);
                }

                primKeys.close();
            }

            allTables.close();
        } catch (SQLException e) {

            // System.out.println("SQL Exception: " + e.getMessage());
        }

        return result;
    }

    // determine the index of the choosen table in Vector vHoldTableNames
    // if the table name is not in vHoldTableNames, create a ZaurusTableForm for it
    private int getChoosenTableIndex() {

        String tableName = cTables.getSelectedItem();

        // System.out.println("in getChoosenTableIndex, selected Item is "+tableName);
        int index = getTableIndex(tableName);

        if (index >= 0) {

            // System.out.println("table found, index: " + index);
            return index;
        }    // end of if (index >= 0)

        ZaurusTableForm tableForm = new ZaurusTableForm(tableName, cConn);

        pForm.add(tableName, tableForm);
        vHoldTableNames.addElement(tableName);
        vHoldForms.addElement(tableForm);

        // System.out.println("new tableform for table "+tableName+", index: " + index);
        return vHoldTableNames.size() - 1;
    }

    // determine the index of the given tableName in Vector vHoldTableNames
    // if the name is not in vHoldTableNames, answer -1
    private int getTableIndex(String tableName) {

        int index;

        // System.out.println("begin searching for "+tableName);
        for (index = 0; index < vHoldTableNames.size(); index++) {

            // System.out.println("in getTableIndex searching for "+tableName+", index: "+index);
            if (tableName.equals((String) vHoldTableNames.elementAt(index))) {
                return index;
            }    // end of if (tableName.equals(vHoldTableNames.elementAt(index)))
        }        // end of for (index = 0; index < vHoldTableNames.size(); index ++)

        return -1;
    }

    // convert the search words in the textfield to an array of words
    private String[] getWords() {

        StringTokenizer tokenizer =
            new StringTokenizer(fSearchWords.getText());
        String[] result = new String[tokenizer.countTokens()];
        int      i      = 0;

        while (tokenizer.hasMoreTokens()) {
            result[i++] = tokenizer.nextToken();
        }    // end of while ((tokenizer.hasMoreTokens()))

        return result;
    }

    // init the three boxes for buttons
    private void initButtons() {

        // the buttons for the search form
        bSearchRow = new Button("Search Rows");
        bNewRow    = new Button("Insert New Row");

        bSearchRow.addActionListener(this);
        bNewRow.addActionListener(this);

        pSearchButs = new Panel();

        pSearchButs.setLayout(new GridLayout(1, 0, 4, 4));
        pSearchButs.add(bSearchRow);
        pSearchButs.add(bNewRow);

        // the buttons for editing a row
        bCancel1         = new Button("Cancel");
        bPrev            = new Button("Prev");
        bNext            = new Button("Next");
        bDelete          = new Button("Delete");
        lastButtonDelete = false;
        bNewSearch       = new Button("Search");

        bCancel1.addActionListener(this);
        bPrev.addActionListener(this);
        bNext.addActionListener(this);
        bDelete.addActionListener(this);
        bNewSearch.addActionListener(this);

        pEditButs = new Panel();

        pEditButs.setLayout(new GridLayout(1, 0, 4, 4));
        pEditButs.add(bCancel1);
        pEditButs.add(bPrev);
        pEditButs.add(bNext);
        pEditButs.add(bDelete);
        pEditButs.add(bNewSearch);

        // the buttons for inserting a new row
        pInsertButs = new Panel();

        pInsertButs.setLayout(new GridLayout(1, 0, 4, 4));

        bCancel2    = new Button("Cancel Insert");
        bNewInsert  = new Button("New Insert");
        bNewSearch1 = new Button("Search");

        bCancel2.addActionListener(this);
        bNewInsert.addActionListener(this);
        bNewSearch1.addActionListener(this);
        pInsertButs.add(bCancel2);
        pInsertButs.add(bNewInsert);
        pInsertButs.add(bNewSearch1);
    }

    // check whether the last button pressed was delete
    // if so, clear status line and reset the flag
    private void resetLastButtonDelete() {

        if (lastButtonDelete) {
            ZaurusEditor.printStatus("");

            lastButtonDelete = false;
        }    // end of if (lastButtonDelete)
    }

    // reset  everything after changes in the database
    private void resetTableForms() {

        lForm.show(pForm, "search");
        lButton.show(pButton, "search");

        Vector vAllTables = getAllTables();

        // fill the drop down list again
        // get all table names and show a drop down list of them in cTables
        cTables.removeAll();

        for (Enumeration e = vAllTables.elements(); e.hasMoreElements(); ) {
            cTables.addItem((String) e.nextElement());
        }

        // remove all form panels from pForm
        for (Enumeration e = vHoldForms.elements(); e.hasMoreElements(); ) {
            pForm.remove((ZaurusTableForm) e.nextElement());
        }    // end of while (Enumeration e = vHoldForms.elements(); e.hasMoreElements();)

        // initialize a new list for the table names which have a form in pForm
        vHoldTableNames = new Vector(20);
        vHoldForms      = new Vector(20);
    }
}
