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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.FileDialog;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Menu;
import java.awt.MenuBar;
import java.awt.MenuItem;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowListener;
import java.awt.image.MemoryImageSource;

import org.hsqldb_voltpatches.lib.java.JavaSystem;

/**
 * Class declaration
 *
 *
 * @version 1.0.0.1
 * @author ulrivo@users
 *
 */
public class ZaurusDatabaseManager extends DatabaseManager
implements ActionListener, WindowListener, KeyListener {

    // (ulrivo): new buttons to switch the cards
    Button butTree;
    Button butCommand;
    Button butResult;
    Button butEditor;

    // (ulrivo): Panel pCard with CardLayout inside Frame fMain
    Panel      pCard;
    CardLayout layoutCard;

    // the editor/input form
    ZaurusEditor eEditor;

    // (ulrivo): variables set by arguments from the commandline
    static String defDriver;
    static String defURL;
    static String defUser;
    static String defPassword;
    static String defQuery;
    static String defDirectory;
    static String defDatabase;
    static int    defWidth  = 237;
    static int    defHeight = 259;
    static int    defLocX   = 0;
    static int    defLocY   = 0;

    /**
     * Method declaration
     *
     *
     * @param c
     */
    public void connect(Connection c) {

        if (c == null) {
            return;
        }

        if (cConn != null) {
            try {
                cConn.close();
            } catch (SQLException e) {}
        }

        cConn = c;

        try {
            dMeta      = cConn.getMetaData();
            sStatement = cConn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        refreshTree();
    }

    /**
     * Method declaration
     *
     *
     * @param arg
     */
    public static void main(String[] arg) {

        bMustExit = true;

        // (ulrivo): read all arguments from the command line
        int i = 0;

        while (i < arg.length) {
            if (arg[i].equalsIgnoreCase("-driver") && (i + 1 < arg.length)) {
                i++;

                defDriver = arg[i];
            } else if (arg[i].equalsIgnoreCase("-url")
                       && (i + 1 < arg.length)) {
                i++;

                defURL = arg[i];
            } else if (arg[i].equalsIgnoreCase("-width")
                       && (i + 1 < arg.length)) {
                i++;

                try {
                    defWidth = Integer.parseInt(arg[i]);
                } catch (Exception e) {}
            } else if (arg[i].equalsIgnoreCase("-height")
                       && (i + 1 < arg.length)) {
                i++;

                try {
                    defHeight = Integer.parseInt(arg[i]);
                } catch (Exception e) {}
            } else if (arg[i].equalsIgnoreCase("-locx")
                       && (i + 1 < arg.length)) {
                i++;

                try {
                    defLocX = Integer.parseInt(arg[i]);
                } catch (Exception e) {}
            } else if (arg[i].equalsIgnoreCase("-locy")
                       && (i + 1 < arg.length)) {
                i++;

                try {
                    defLocY = Integer.parseInt(arg[i]);
                } catch (Exception e) {}
            } else if (arg[i].equalsIgnoreCase("-user")
                       && (i + 1 < arg.length)) {
                i++;

                defUser = arg[i];
            } else if (arg[i].equalsIgnoreCase("-password")
                       && (i + 1 < arg.length)) {
                i++;

                defPassword = arg[i];
            } else if (arg[i].equalsIgnoreCase("-query")
                       && (i + 1 < arg.length)) {
                i++;

                defQuery = arg[i];
            } else if (arg[i].equalsIgnoreCase("-defDirectory")
                       && (i + 1 < arg.length)) {
                i++;

                defDirectory = arg[i];
            } else if (arg[i].equalsIgnoreCase("-database")
                       && (i + 1 < arg.length)) {
                i++;

                defDatabase = arg[i];
            } else {
                showUsage();

                return;
            }

            i++;
        }

        ZaurusDatabaseManager m = new ZaurusDatabaseManager();

        m.main();

        // (ulrivo): make default connection if arguments set via the command line
        Connection c = null;

        if ((defDriver != null && defURL != null) || (defDatabase != null)) {
            if (defDatabase != null) {
                defDriver   = "org.hsqldb_voltpatches.jdbcDriver";
                defURL      = "jdbc:hsqldb:" + defDatabase;
                defUser     = "SA";
                defPassword = "";
            }

            try {
                Class.forName(defDriver).newInstance();

                c = DriverManager.getConnection(defURL, defUser, defPassword);
            } catch (Exception e) {
                System.out.println("No connection for " + defDriver + " at "
                                   + defURL);
                e.printStackTrace();
            }
        } else {
            c = ZaurusConnectionDialog.createConnection(m.fMain, "Connect",
                    new Insets(defWidth, defHeight, defLocX, defLocY));
        }

        if (c == null) {
            return;
        }

        m.connect(c);
    }

    private static void showUsage() {

        System.out.println(
            "Usage: java org.hsqldb_voltpatches.util.ZaurusDatabaseManager [options]");
        System.out.println("where options could be:");
        System.out.println(
            "If the next two options are set, the specified connection will be used:");
        System.out.println("   -driver dr");
        System.out.println("   -url address");
        System.out.println("-user name");
        System.out.println("-password passw");
        System.out.println("Alternative the database argument is used,");
        System.out.println(
            "and the hsqldb Driver Standalone is choosen for user 'SA'.");
        System.out.println("-database db");
        System.out.println(
            "-query qu                   the query qu will be read during initialization");
        System.out.println(
            "-defaultDirectory defdir    default dir for the file open dialog");
        System.out.println(
            "If the next two options are set, the frame will be set to the specified values:");
        System.out.println("   -width width");
        System.out.println("   -height height");
        System.out.println("-locX positon left ");
        System.out.println("-locY positon top ");
        System.out.println("");
        System.out.println(
            "1. Example: java org.hsqldb_voltpatches.util.ZaurusDatabaseManager +");
        System.out.println("  -driver 'org.hsqldb_voltpatches.jdbcDriver' +");
        System.out.println("  -url 'jdbc:hsqldb:test'");
        System.out.println(
            "2. Example: java org.hsqldb_voltpatches.util.ZaurusDatabaseManager +");
        System.out.println("  -database 'test'");
    }

    /**
     * Method declaration
     *
     */
    public void main() {

        fMain = new Frame("HSQLDB Database Manager for Zaurus");
        imgEmpty = createImage(new MemoryImageSource(2, 2, new int[4 * 4], 2,
                2));

        fMain.setIconImage(imgEmpty);
        fMain.addWindowListener(this);

        MenuBar bar = new MenuBar();

        // no shortcuts used
        String[] fitems = {
            "-Connect...", "--", "-Open Script...", "-Save Script...",
            "-Save Result...", "--", "-Exit"
        };

        addMenu(bar, "File", fitems);

        String[] vitems = {
            "-Refresh Tree", "--", "-View Tree", "-View Command",
            "-View Result", "-View Editor", "--", "-Results in Grid",
            "-Results in Text"
        };

        addMenu(bar, "View", vitems);

        String[] sitems = {
            "-SELECT", "-INSERT", "-UPDATE", "-DELETE", "--", "-CREATE TABLE",
            "-DROP TABLE", "-CREATE INDEX", "-DROP INDEX", "--", "-SCRIPT",
            "-SHUTDOWN", "--", "-Test Script"
        };

        addMenu(bar, "SQL", sitems);

        Menu recent = new Menu("Recent");

        mRecent = new Menu("Recent");

        bar.add(mRecent);

        String[] soptions = {
            "-AutoCommit on", "-AutoCommit off", "-Commit", "-Rollback", "--",
            "-Disable MaxRows", "-Set MaxRows to 100", "--", "-Logging on",
            "-Logging off", "--",
            "-Insert test data"    // , "-Transfer"
        };

        addMenu(bar, "Options", soptions);

        String[] shelp = { "-Show HTML-Help in browser" };

        addMenu(bar, "?", shelp);
        fMain.setMenuBar(bar);
        fMain.setSize(defWidth, defHeight);
        fMain.add("Center", this);
        initGUI();

        sRecent = new String[iMaxRecent];

        Dimension d    = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = fMain.getSize();

        // (ulrivo): arguments from command line or
        // full size on screen with less than 640 width
        if (d.width > 640) {
            fMain.setLocation((d.width - size.width) / 2,
                              (d.height - size.height) / 2);
        } else if (defWidth > 0 && defHeight > 0) {
            fMain.setLocation(defLocX, defLocY);
            fMain.setSize(defWidth, defHeight);
        } else {
            fMain.setLocation(0, 0);
            fMain.setSize(d);
        }

        fMain.show();

        // (ulrivo): load query from command line
        if (defQuery != null) {
            txtCommand.setText(DatabaseManagerCommon.readFile(defQuery));
        }

        txtCommand.requestFocus();
    }

    /**
     * Method declaration
     *
     *
     * @param k
     */
    public void keyTyped(KeyEvent k) {

        // Strg+Enter or Shift+Enter executes the actual SQL statement in command panel
        if (k.getKeyChar() == '\n'
                && (k.isControlDown() || k.isShiftDown())) {
            k.consume();
            execute();
            layoutCard.show(pCard, "result");
        }
    }

    public void keyPressed(KeyEvent k) {

        //  System.out.println("Key pressed: " + k.getKeyCode());
    }

    /**
     * Method declaration
     *
     *
     * @param ev
     */
    public void actionPerformed(ActionEvent ev) {

        String s = ev.getActionCommand();

        if (s == null) {
            if (ev.getSource() instanceof MenuItem) {
                MenuItem i;

                s = ((MenuItem) ev.getSource()).getLabel();
            }
        }

        if (s.equals("Execute")) {
            execute();
            layoutCard.show(pCard, "result");
        } else if (s.equals("Tree")) {
            layoutCard.show(pCard, "tree");
        } else if (s.equals("Command")) {
            layoutCard.show(pCard, "command");
        } else if (s.equals("Result")) {
            layoutCard.show(pCard, "result");
        } else if (s.equals("Editor")) {
            layoutCard.show(pCard, "editor");
        } else if (s.equals("Exit")) {
            windowClosing(null);
        } else if (s.equals("Logging on")) {
            JavaSystem.setLogToSystem(true);
        } else if (s.equals("Logging off")) {
            JavaSystem.setLogToSystem(false);
        } else if (s.equals("Refresh Tree")) {
            refreshTree();
            layoutCard.show(pCard, "tree");
        } else if (s.startsWith("#")) {
            int i = Integer.parseInt(s.substring(1));

            txtCommand.setText(sRecent[i]);
        } else if (s.equals("Connect...")) {
            connect(ZaurusConnectionDialog.createConnection(fMain, "Connect",
                    new Insets(defWidth, defHeight, defLocX, defLocY)));
            refreshTree();
            layoutCard.show(pCard, "tree");
        } else if (s.equals("View Tree")) {
            layoutCard.show(pCard, "tree");
        } else if (s.equals("View Command")) {
            layoutCard.show(pCard, "command");
        } else if (s.equals("View Result")) {
            layoutCard.show(pCard, "result");
        } else if (s.equals("View Editor")) {
            layoutCard.show(pCard, "editor");
        } else if (s.equals("Results in Grid")) {
            iResult = 0;

            pResult.removeAll();
            pResult.add("Center", gResult);
            pResult.doLayout();
            layoutCard.show(pCard, "result");
        } else if (s.equals("Open Script...")) {
            FileDialog f = new FileDialog(fMain, "Open Script",
                                          FileDialog.LOAD);

            // (ulrivo): set default directory if set from command line
            if (defDirectory != null) {
                f.setDirectory(defDirectory);
            }

            f.show();

            String file = f.getFile();

            if (file != null) {
                txtCommand.setText(
                    DatabaseManagerCommon.readFile(f.getDirectory() + file));
            }

            layoutCard.show(pCard, "command");
        } else if (s.equals("Save Script...")) {
            FileDialog f = new FileDialog(fMain, "Save Script",
                                          FileDialog.SAVE);

            // (ulrivo): set default directory if set from command line
            if (defDirectory != null) {
                f.setDirectory(defDirectory);
            }

            f.show();

            String file = f.getFile();

            if (file != null) {
                DatabaseManagerCommon.writeFile(f.getDirectory() + file,
                                                txtCommand.getText());
            }
        } else if (s.equals("Save Result...")) {
            FileDialog f = new FileDialog(fMain, "Save Result",
                                          FileDialog.SAVE);

            // (ulrivo): set default directory if set from command line
            if (defDirectory != null) {
                f.setDirectory(defDirectory);
            }

            f.show();

            String file = f.getFile();

            if (file != null) {
                showResultInText();
                DatabaseManagerCommon.writeFile(f.getDirectory() + file,
                                                txtResult.getText());
            }
        } else if (s.equals("Results in Text")) {
            iResult = 1;

            pResult.removeAll();
            pResult.add("Center", txtResult);
            pResult.doLayout();
            showResultInText();
            layoutCard.show(pCard, "result");
        } else if (s.equals("AutoCommit on")) {
            try {
                cConn.setAutoCommit(true);
            } catch (SQLException e) {}
        } else if (s.equals("AutoCommit off")) {
            try {
                cConn.setAutoCommit(false);
            } catch (SQLException e) {}
        } else if (s.equals("Commit")) {
            try {
                cConn.commit();
            } catch (SQLException e) {}
        } else if (s.equals("Insert test data")) {
            insertTestData();
            layoutCard.show(pCard, "result");
        } else if (s.equals("Rollback")) {
            try {
                cConn.rollback();
            } catch (SQLException e) {}
        } else if (s.equals("Disable MaxRows")) {
            try {
                sStatement.setMaxRows(0);
            } catch (SQLException e) {}
        } else if (s.equals("Set MaxRows to 100")) {
            try {
                sStatement.setMaxRows(100);
            } catch (SQLException e) {}
        } else if (s.equals("SELECT")) {
            showHelp(DatabaseManagerCommon.selectHelp);
        } else if (s.equals("INSERT")) {
            showHelp(DatabaseManagerCommon.insertHelp);
        } else if (s.equals("UPDATE")) {
            showHelp(DatabaseManagerCommon.updateHelp);
        } else if (s.equals("DELETE")) {
            showHelp(DatabaseManagerCommon.deleteHelp);
        } else if (s.equals("CREATE TABLE")) {
            showHelp(DatabaseManagerCommon.createTableHelp);
        } else if (s.equals("DROP TABLE")) {
            showHelp(DatabaseManagerCommon.dropTableHelp);
        } else if (s.equals("CREATE INDEX")) {
            showHelp(DatabaseManagerCommon.createIndexHelp);
        } else if (s.equals("DROP INDEX")) {
            showHelp(DatabaseManagerCommon.dropIndexHelp);
        } else if (s.equals("CHECKPOINT")) {
            showHelp(DatabaseManagerCommon.checkpointHelp);
        } else if (s.equals("SCRIPT")) {
            showHelp(DatabaseManagerCommon.scriptHelp);
        } else if (s.equals("SHUTDOWN")) {
            showHelp(DatabaseManagerCommon.shutdownHelp);
        } else if (s.equals("SET")) {
            showHelp(DatabaseManagerCommon.setHelp);
        } else if (s.equals("Test Script")) {
            showHelp(DatabaseManagerCommon.testHelp);
        } else if (s.equals("Show HTML-Help in browser")) {
            try {
                System.out.println("Starting Opera on index.html");
                Runtime.getRuntime().exec(new String[] {
                    "opera", "/home/QtPalmtop/help/html/hsqldb/index.html"
                });
            } catch (IOException e) {
                System.out.println("A problem with Opera occured.");
            }
        }
    }

    /**
     * Method declaration
     *
     */
    private void initGUI() {

        Panel pQuery   = new Panel();
        Panel pCommand = new Panel();

        // define a Panel pCard which takes four different cards/views:
        // tree of tables, command SQL text area, result window and an editor/input form
        pCard      = new Panel();
        layoutCard = new CardLayout(2, 2);

        pCard.setLayout(layoutCard);

        // four buttons at the top to quickly switch between the four views
        butTree    = new Button("Tree");
        butCommand = new Button("Command");
        butResult  = new Button("Result");
        butEditor  = new Button("Editor");

        butTree.addActionListener(this);
        butCommand.addActionListener(this);
        butResult.addActionListener(this);
        butEditor.addActionListener(this);

        Panel pButtons = new Panel();

        pButtons.setLayout(new GridLayout(1, 4, 8, 8));
        pButtons.add(butTree);
        pButtons.add(butCommand);
        pButtons.add(butResult);
        pButtons.add(butEditor);

        pResult = new Panel();

        pQuery.setLayout(new BorderLayout());
        pCommand.setLayout(new BorderLayout());
        pResult.setLayout(new BorderLayout());

        Font fFont = new Font("Dialog", Font.PLAIN, 12);

        txtCommand = new TextArea(5, 40);

        txtCommand.addKeyListener(this);

        txtResult = new TextArea(20, 40);

        txtCommand.setFont(fFont);
        txtResult.setFont(new Font("Courier", Font.PLAIN, 12));

        butExecute = new Button("Execute");

        butExecute.addActionListener(this);
        pCommand.add("South", butExecute);
        pCommand.add("Center", txtCommand);

        gResult = new Grid();

        setLayout(new BorderLayout());
        pResult.add("Center", gResult);

        tTree = new Tree();

        tTree.setMinimumSize(new Dimension(200, 100));
        gResult.setMinimumSize(new Dimension(200, 300));

        eEditor = new ZaurusEditor();

        pCard.add("tree", tTree);
        pCard.add("command", pCommand);
        pCard.add("result", pResult);
        pCard.add("editor", eEditor);
        fMain.add("Center", pCard);
        fMain.add("North", pButtons);
        doLayout();
        fMain.pack();
    }

    protected void refreshTree() {
        super.refreshTree();
        eEditor.refresh(cConn);
    }
}
