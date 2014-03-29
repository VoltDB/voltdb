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

import java.applet.Applet;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FileDialog;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Image;
import java.awt.Menu;
import java.awt.MenuBar;
import java.awt.MenuItem;
import java.awt.MenuShortcut;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.awt.image.MemoryImageSource;

import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.lib.RCData;

// sqlbob@users 20020401 - patch 1.7.0 by sqlbob (RMP) - enhancements
// sqlbob@users 20020401 - patch 537501 by ulrivo - command line arguments
// sqlbob@users 20020407 - patch 1.7.0 - reengineering
// nickferguson@users 20021005 - patch 1.7.1 - enhancements
/*
 * unsaved@users 20050426 - Switched default switch method from "-switch" to
 * "--switch" because "-switch" usage is ambiguous as used here.  Single
 * switches should be reserved for single-letter switches which can be mixed
 * like * "-u -r -l" = "-url".  -blaine
*/

/**
 * AWT Tool for manageing a JDBC database.<p>
 * <pre>
 *             Usage: java DatabaseManagerSwing [--options]
 *             where options include:
 *              --driver <classname>  jdbc driver class
 *              --url <name>          jdbc url
 *              --user <name>         username used for connection
 *              --password <password> password for this user
 *              --urlid <urlid>       get connection info from RC file
 *              --rcfile <file>       use instead of default (with urlid)
 *              --dir <path>          default directory
 *              --script <file>       reads from script file
 *</pre>
 *
 * Originally in HypersonicSQL. Extended in various versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class DatabaseManager extends Applet
implements ActionListener, WindowListener, KeyListener {

    private static final String DEFAULT_RCFILE =
        System.getProperty("user.home") + "/dbmanager.rc";
    static final String    NL           = System.getProperty("line.separator");
    static final int       iMaxRecent   = 24;
    private static boolean TT_AVAILABLE = false;

//#ifdef JAVA2FULL
    static {
        try {
            Class.forName(DatabaseManager.class.getPackage().getName()
                          + ".Transfer");

            TT_AVAILABLE = true;
        } catch (Throwable t) {}
    }

//#endif
    private static final String HELP_TEXT =
        "See the forums, mailing lists, and HSQLDB User Guide\n"
        + "at http://hsqldb.org.\n\n"
        + "Please paste the following version identifier with any\n"
        + "problem reports or help requests:  $Revision: 2928 $"
        + (TT_AVAILABLE ? ""
                        : ("\n\nTransferTool classes are not in CLASSPATH.\n"
                           + "To enable the Tools menu, add 'transfer.jar' to your class path."));
    ;
    private static final String ABOUT_TEXT =
        "$Revision: 2928 $ of DatabaseManagerSwing\n\n"
        + "Copyright (c) 1995-2000, The Hypersonic SQL Group.\n"
        + "Copyright (c) 2001-2009, The HSQL Development Group.\n"
        + "http://hsqldb.org  (User Guide available at this site).\n\n\n"
        + "You may use and redistribute according to the HSQLDB\n"
        + "license documented in the source code and at the web\n"
        + "site above."          //
        + (TT_AVAILABLE ? "\n\nTransferTool options are available."
                        : "");
    Connection       cConn;
    DatabaseMetaData dMeta;
    Statement        sStatement;
    Menu             mRecent;
    String[]         sRecent;
    int              iRecent;
    TextArea         txtCommand;
    Button           butExecute;
    Button           butClear;
    Tree             tTree;
    Panel            pResult;
    long             lTime;
    int              iResult;    // 0: grid; 1: text
    Grid             gResult;
    TextArea         txtResult;
    boolean          bHelp;
    Frame            fMain;
    Image            imgEmpty;
    static boolean   bMustExit;
    String           ifHuge = "";

    // (ulrivo): variables set by arguments from the commandline
    static String defDriver   = "org.hsqldb_voltpatches.jdbcDriver";
    static String defURL      = "jdbc:hsqldb:mem:.";
    static String defUser     = "SA";
    static String defPassword = "";
    static String defScript;
    static String defDirectory;

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

            refreshTree();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void init() {

        DatabaseManager m = new DatabaseManager();

        m.main();

        try {
            m.connect(ConnectionDialog.createConnection(defDriver, defURL,
                    defUser, defPassword));
            m.insertTestData();
            m.refreshTree();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Run with --help switch for usage instructions.
     *
     * @throws IllegalArgumentException for the obvious reason
     */
    public static void main(String[] arg) {

        System.getProperties().put("sun.java2d.noddraw", "true");

        // (ulrivo): read all arguments from the command line
        String  lowerArg;
        String  urlid        = null;
        String  rcFile       = null;
        boolean autoConnect  = false;
        boolean urlidConnect = false;

        bMustExit = true;

        for (int i = 0; i < arg.length; i++) {
            lowerArg = arg[i].toLowerCase();

            if (lowerArg.startsWith("--")) {
                lowerArg = lowerArg.substring(1);
            }

            i++;

            if (i == arg.length) {
                showUsage();

                return;
            }

            if (lowerArg.equals("-driver")) {
                defDriver   = arg[i];
                autoConnect = true;
            } else if (lowerArg.equals("-url")) {
                defURL      = arg[i];
                autoConnect = true;
            } else if (lowerArg.equals("-user")) {
                defUser     = arg[i];
                autoConnect = true;
            } else if (lowerArg.equals("-password")) {
                defPassword = arg[i];
                autoConnect = true;
            } else if (lowerArg.equals("-urlid")) {
                urlid        = arg[i];
                urlidConnect = true;
            } else if (lowerArg.equals("-rcfile")) {
                rcFile       = arg[i];
                urlidConnect = true;
            } else if (lowerArg.equals("-dir")) {
                defDirectory = arg[i];
            } else if (lowerArg.equals("-script")) {
                defScript = arg[i];
            } else if (lowerArg.equals("-noexit")) {
                bMustExit = false;

                i--;
            } else if (lowerArg.equals("-help")) {
                showUsage();

                return;
            } else {
                /* Syntax ERRORS should either throw or exit with non-0 status.
                 * In our case, it may be unsafe to exit, so we throw.
                 * (I.e. should provide easy way for caller to programmatically
                 * determine that there was an invocation problem).
                 */
                throw new IllegalArgumentException(
                    "Try:  java... " + DatabaseManagerSwing.class.getName()
                    + " --help");

                // No reason to localize, since the main syntax message is
                // not localized.
            }
        }

        DatabaseManager m = new DatabaseManager();

        m.main();

        Connection c = null;

        try {
            if (autoConnect && urlidConnect) {
                throw new IllegalArgumentException(
                    "You may not specify both (urlid) AND (url/user/password).");
            }

            if (autoConnect) {
                c = ConnectionDialog.createConnection(defDriver, defURL,
                                                      defUser, defPassword);
            } else if (urlidConnect) {
                if (urlid == null) {
                    throw new IllegalArgumentException(
                        "You must specify an 'urlid' to use an RC file");
                }

                autoConnect = true;

                if (rcFile == null) {
                    rcFile = DEFAULT_RCFILE;
                }

                c = new RCData(new File(rcFile), urlid).getConnection(null,
                               System.getProperty("sqlfile.charset"),
                               System.getProperty("javax.net.ssl.trustStore"));
            } else {
                c = ConnectionDialog.createConnection(m.fMain, "Connect");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (c == null) {
            return;
        }

        m.connect(c);
    }

    private static void showUsage() {

        System.out.println(
            "Usage: java DatabaseManager [--options]\n"
            + "where options include:\n"
            + "    --help                show this message\n"
            + "    --driver <classname>  jdbc driver class\n"
            + "    --url <name>          jdbc url\n"
            + "    --user <name>         username used for connection\n"
            + "    --password <password> password for this user\n"
            + "    --urlid <urlid>       use url/user/password/driver in rc file\n"
            + "    --rcfile <file>       (defaults to 'dbmanager.rc' in home dir)\n"
            + "    --dir <path>          default directory\n"
            + "    --script <file>       reads from script file\n"
            + "    --noexit              do not call system.exit()");
    }

    void insertTestData() {

        try {
            DatabaseManagerCommon.createTestTables(sStatement);
            refreshTree();
            txtCommand.setText(
                DatabaseManagerCommon.createTestData(sStatement));
            refreshTree();

            for (int i = 0; i < DatabaseManagerCommon.testDataSql.length;
                    i++) {
                addToRecent(DatabaseManagerCommon.testDataSql[i]);
            }

            execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void main() {

        fMain = new Frame("HSQL Database Manager");
        imgEmpty = createImage(new MemoryImageSource(2, 2, new int[4 * 4], 2,
                2));

        fMain.setIconImage(imgEmpty);
        fMain.addWindowListener(this);

        MenuBar bar = new MenuBar();

        // used shortcuts: CERGTSIUDOLM
        String[] fitems = {
            "-Connect...", "--", "-Open Script...", "-Save Script...",
            "-Save Result...", "-Save Result csv...", "--", "-Exit"
        };

        addMenu(bar, "File", fitems);

        String[] vitems = {
            "RRefresh Tree", "--", "GResults in Grid", "TResults in Text",
            "--", "1Shrink Tree", "2Enlarge Tree", "3Shrink Command",
            "4Enlarge Command"
        };

        addMenu(bar, "View", vitems);

        String[] sitems = {
            "SSELECT", "IINSERT", "UUPDATE", "DDELETE", "--", "-CREATE TABLE",
            "-DROP TABLE", "-CREATE INDEX", "-DROP INDEX", "--", "-CHECKPOINT",
            "-SCRIPT", "-SET", "-SHUTDOWN", "--", "-Test Script"
        };

        addMenu(bar, "Command", sitems);

        Menu recent = new Menu("Recent");

        mRecent = new Menu("Recent");

        bar.add(mRecent);

        String[] soptions = {
            "-AutoCommit on", "-AutoCommit off", "OCommit", "LRollback", "--",
            "-Disable MaxRows", "-Set MaxRows to 100", "--", "-Logging on",
            "-Logging off", "--", "-Insert test data"
        };

        addMenu(bar, "Options", soptions);

        String[] stools = {
            "-Dump", "-Restore", "-Transfer"
        };

        addMenu(bar, "Tools", stools);

        Menu     hMenu = new Menu("Help");
        MenuItem aItem = new MenuItem("About");

        aItem.setShortcut(new MenuShortcut('A'));
        aItem.addActionListener(this);
        hMenu.add(aItem);

        MenuItem hItem = new MenuItem("Help");

        hItem.setShortcut(new MenuShortcut('H'));
        hItem.addActionListener(this);
        hMenu.add(hItem);

        //bar.add(hMenu);
        // Command above disabled only until a help display bug is fixed.
        fMain.setMenuBar(bar);
        fMain.setSize(640, 480);
        fMain.add("Center", this);
        initGUI();

        sRecent = new String[iMaxRecent];

        Dimension d    = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = fMain.getSize();

        // (ulrivo): full size on screen with less than 640 width
        if (d.width >= 640) {
            fMain.setLocation((d.width - size.width) / 2,
                              (d.height - size.height) / 2);
        } else {
            fMain.setLocation(0, 0);
            fMain.setSize(d);
        }

        fMain.show();

        // (ulrivo): load query from command line
        if (defScript != null) {
            if (defDirectory != null) {
                defScript = defDirectory + File.separator + defScript;
            }

            txtCommand.setText(DatabaseManagerCommon.readFile(defScript));
        }

        txtCommand.requestFocus();
    }

    void addMenu(MenuBar b, String name, String[] items) {

        /* It's a very poor design to encapsulate menu creation this way.
         * Can't customize the menus this way (e.g. shortcut keys,
         * mnemonics, disabling, etc. */
        Menu menu = new Menu(name);

        if (name.equals("Tools") && !TT_AVAILABLE) {

            // Terrible place to do this.  Forced to due to method design.
            menu.setEnabled(false);
        }

        addMenuItems(menu, items);
        b.add(menu);
    }

    void addMenuItems(Menu f, String[] m) {

        for (int i = 0; i < m.length; i++) {
            MenuItem item = new MenuItem(m[i].substring(1));
            char     c    = m[i].charAt(0);

            if (c != '-') {
                item.setShortcut(new MenuShortcut(c));
            }

            item.addActionListener(this);
            f.add(item);
        }
    }

    public void keyPressed(KeyEvent k) {}

    public void keyReleased(KeyEvent k) {}

    public void keyTyped(KeyEvent k) {

        if (k.getKeyChar() == '\n' && k.isControlDown()) {
            k.consume();
            execute();
        }
    }

    public void actionPerformed(ActionEvent ev) {

        String s = ev.getActionCommand();

        if (s == null) {
            if (ev.getSource() instanceof MenuItem) {
                MenuItem i;

                s = ((MenuItem) ev.getSource()).getLabel();
            }
        }

        if (s == null) {}
        else if (s.equals("Execute")) {
            execute();
        } else if (s.equals("Clear")) {
            clear();
        } else if (s.equals("Exit")) {
            windowClosing(null);
        } else if (s.equals("Transfer")) {
            Transfer.work(null);
        } else if (s.equals("Dump")) {
            Transfer.work(new String[]{ "-d" });
        } else if (s.equals("Restore")) {
            Transfer.work(new String[]{ "-r" });
            refreshTree();
        } else if (s.equals("Logging on")) {
            JavaSystem.setLogToSystem(true);
        } else if (s.equals("Logging off")) {
            JavaSystem.setLogToSystem(false);
        } else if (s.equals("Help")) {
            showHelp(new String[] {
                "", HELP_TEXT
            });
        } else if (s.equals("About")) {
            showHelp(new String[] {
                "", ABOUT_TEXT
            });
        } else if (s.equals("Refresh Tree")) {
            refreshTree();
        } else if (s.startsWith("#")) {
            int i = Integer.parseInt(s.substring(1));

            txtCommand.setText(sRecent[i]);
        } else if (s.equals("Connect...")) {
            connect(ConnectionDialog.createConnection(fMain, "Connect"));
            refreshTree();
        } else if (s.equals("Results in Grid")) {
            iResult = 0;

            pResult.removeAll();
            pResult.add("Center", gResult);
            pResult.doLayout();
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
                StringBuffer buf = new StringBuffer();

                ifHuge = DatabaseManagerCommon.readFile(f.getDirectory()
                        + file);

                if (4096 <= ifHuge.length()) {
                    buf.append(
                        "This huge file cannot be edited.\n Please execute or clear\n");
                    txtCommand.setText(buf.toString());
                } else {
                    txtCommand.setText(ifHuge);
                }
            }
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
        } else if (s.equals("Save Result csv...")) {
            FileDialog f = new FileDialog(fMain, "Save Result CSV",
                                          FileDialog.SAVE);

            // (ulrivo): set default directory if set from command line
            if (defDirectory != null) {
                f.setDirectory(defDirectory);
            }

            f.show();

            String dir  = f.getDirectory();
            String file = f.getFile();

            if (dir != null) {
                file = dir + "/" + file;
            }

            if (file != null) {
                showResultInText();
                saveAsCsv(file);
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
        } else if (s.equals("AutoCommit on")) {
            try {
                cConn.setAutoCommit(true);
            } catch (SQLException e) {}
        } else if (s.equals("AutoCommit off")) {
            try {
                cConn.setAutoCommit(false);
            } catch (SQLException e) {}
        } else if (s.equals("Enlarge Tree")) {
            Dimension d = tTree.getMinimumSize();

            d.width += 20;

            tTree.setMinimumSize(d);
            fMain.pack();
        } else if (s.equals("Shrink Tree")) {
            Dimension d = tTree.getMinimumSize();

            d.width -= 20;

            if (d.width >= 0) {
                tTree.setMinimumSize(d);
            }

            fMain.pack();
        } else if (s.equals("Enlarge Command")) {
            txtCommand.setRows(txtCommand.getRows() + 1);
            fMain.pack();
        } else if (s.equals("Shrink Command")) {
            int i = txtCommand.getRows() - 1;

            txtCommand.setRows(i < 1 ? 1
                                     : i);
            fMain.pack();
        } else if (s.equals("Commit")) {
            try {
                cConn.commit();
            } catch (SQLException e) {}
        } else if (s.equals("Insert test data")) {
            insertTestData();
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
        }
    }

    void showHelp(String[] help) {

        txtCommand.setText(help[0]);
        txtResult.setText(help[1]);

        bHelp = true;

        pResult.removeAll();
        pResult.add("Center", txtResult);
        pResult.doLayout();
        txtCommand.requestFocus();
        txtCommand.setCaretPosition(help[0].length());
    }

    public void windowActivated(WindowEvent e) {}

    public void windowDeactivated(WindowEvent e) {}

    public void windowClosed(WindowEvent e) {}

    public void windowClosing(WindowEvent ev) {

        try {
            if (cConn != null) {
                cConn.close();
            }
        } catch (Exception e) {}

        fMain.dispose();

        if (bMustExit) {
            System.exit(0);
        }
    }

    public void windowDeiconified(WindowEvent e) {}

    public void windowIconified(WindowEvent e) {}

    public void windowOpened(WindowEvent e) {}

    /**
     * Clear SQL Statements.
     */
    void clear() {

        ifHuge = "";

        txtCommand.setText(ifHuge);
    }

    /**
     * Adjust this method for large strings...ie multi megabtypes.
     */
    void execute() {

        String sCmd = null;

        if (4096 <= ifHuge.length()) {
            sCmd = ifHuge;
        } else {
            sCmd = txtCommand.getText();
        }

        if (sCmd.startsWith("-->>>TEST<<<--")) {
            testPerformance();

            return;
        }

        String[] g = new String[1];

        lTime = System.currentTimeMillis();

        try {
            if (sStatement == null) {
                return;
            }

            sStatement.execute(sCmd);

            lTime = System.currentTimeMillis() - lTime;

            int r = sStatement.getUpdateCount();

            if (r == -1) {
                formatResultSet(sStatement.getResultSet());
            } else {
                g[0] = "update count";

                gResult.setHead(g);

                g[0] = String.valueOf(r);

                gResult.addRow(g);
            }

            addToRecent(txtCommand.getText());
        } catch (SQLException e) {
            lTime = System.currentTimeMillis() - lTime;
            g[0]  = "SQL Error";

            gResult.setHead(g);

            String s = e.getMessage();

            s    += " / Error Code: " + e.getErrorCode();
            s    += " / State: " + e.getSQLState();
            g[0] = s;

            gResult.addRow(g);
        }

        updateResult();
        System.gc();
    }

    void updateResult() {

        if (iResult == 0) {

            // in case 'help' has removed the grid
            if (bHelp) {
                pResult.removeAll();
                pResult.add("Center", gResult);
                pResult.doLayout();

                bHelp = false;
            }

            gResult.update();
            gResult.repaint();
        } else {
            showResultInText();
        }

        txtCommand.selectAll();
        txtCommand.requestFocus();
    }

    void formatResultSet(ResultSet r) {

        if (r == null) {
            String[] g = new String[1];

            g[0] = "Result";

            gResult.setHead(g);

            g[0] = "(empty)";

            gResult.addRow(g);

            return;
        }

        try {
            ResultSetMetaData m   = r.getMetaData();
            int               col = m.getColumnCount();
            String[]          h   = new String[col];

            for (int i = 1; i <= col; i++) {
                h[i - 1] = m.getColumnLabel(i);
            }

            gResult.setHead(h);

            while (r.next()) {
                for (int i = 1; i <= col; i++) {
                    try {
                        h[i - 1] = r.getString(i);

                        if (r.wasNull()) {
                            h[i - 1] = "(null)";
                        }
                    } catch (SQLException e) {}
                }

                gResult.addRow(h);
            }

            r.close();
        } catch (SQLException e) {}
    }

    void testPerformance() {

        String       all   = txtCommand.getText();
        StringBuffer b     = new StringBuffer();
        long         total = 0;

        for (int i = 0; i < all.length(); i++) {
            char c = all.charAt(i);

            if (c != '\n') {
                b.append(c);
            }
        }

        all = b.toString();

        String[] g = new String[4];

        g[0] = "ms";
        g[1] = "count";
        g[2] = "sql";
        g[3] = "error";

        gResult.setHead(g);

        int max = 1;

        lTime = System.currentTimeMillis() - lTime;

        while (!all.equals("")) {
            int    i = all.indexOf(';');
            String sql;

            if (i != -1) {
                sql = all.substring(0, i);
                all = all.substring(i + 1);
            } else {
                sql = all;
                all = "";
            }

            if (sql.startsWith("--#")) {
                max = Integer.parseInt(sql.substring(3));

                continue;
            } else if (sql.startsWith("--")) {
                continue;
            }

            g[2] = sql;

            long l = 0;

            try {
                l = DatabaseManagerCommon.testStatement(sStatement, sql, max);
                total += l;
                g[0]  = String.valueOf(l);
                g[1]  = String.valueOf(max);
                g[3]  = "";
            } catch (SQLException e) {
                g[0] = g[1] = "n/a";
                g[3] = e.toString();
            }

            gResult.addRow(g);
            System.out.println(l + " ms : " + sql);
        }

        g[0] = "" + total;
        g[1] = "total";
        g[2] = "";

        gResult.addRow(g);

        lTime = System.currentTimeMillis() - lTime;

        updateResult();
    }

    void saveAsCsv(String filename) {

        try {
            File      file   = new File(filename);
            CSVWriter writer = new CSVWriter(file, null);
            String[]  col    = gResult.getHead();
            int       width  = col.length;
            Vector    data   = gResult.getData();
            String[]  row;
            int       height = data.size();

            writer.writeHeader(col);

            for (int i = 0; i < height; i++) {
                row = (String[]) data.elementAt(i);

                String[] myRow = new String[row.length];

                for (int j = 0; j < row.length; j++) {
                    String r = row[j];

                    if (r.equals("(null)")) {

                        // null is formatted as (null)
                        r = "";
                    }

                    myRow[j] = r;
                }

                writer.writeData(myRow);
            }

            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("IOError: " + e.getMessage());
        }
    }

    void showResultInText() {

        String[] col   = gResult.getHead();
        int      width = col.length;
        int[]    size  = new int[width];
        Vector   data  = gResult.getData();
        String[] row;
        int      height = data.size();

        for (int i = 0; i < width; i++) {
            size[i] = col[i].length();
        }

        for (int i = 0; i < height; i++) {
            row = (String[]) data.elementAt(i);

            for (int j = 0; j < width; j++) {
                int l = row[j].length();

                if (l > size[j]) {
                    size[j] = l;
                }
            }
        }

        StringBuffer b = new StringBuffer();

        for (int i = 0; i < width; i++) {
            b.append(col[i]);

            for (int l = col[i].length(); l <= size[i]; l++) {
                b.append(' ');
            }
        }

        b.append(NL);

        for (int i = 0; i < width; i++) {
            for (int l = 0; l < size[i]; l++) {
                b.append('-');
            }

            b.append(' ');
        }

        b.append(NL);

        for (int i = 0; i < height; i++) {
            row = (String[]) data.elementAt(i);

            for (int j = 0; j < width; j++) {
                b.append(row[j]);

                for (int l = row[j].length(); l <= size[j]; l++) {
                    b.append(' ');
                }
            }

            b.append(NL);
        }

        b.append(NL + height + " row(s) in " + lTime + " ms");
        txtResult.setText(b.toString());
    }

    private void addToRecent(String s) {

        for (int i = 0; i < iMaxRecent; i++) {
            if (s.equals(sRecent[i])) {
                return;
            }
        }

        if (sRecent[iRecent] != null) {
            mRecent.remove(iRecent);
        }

        sRecent[iRecent] = s;

        if (s.length() > 43) {
            s = s.substring(0, 40) + "...";
        }

        MenuItem item = new MenuItem(s);

        item.setActionCommand("#" + iRecent);
        item.addActionListener(this);
        mRecent.insert(item, iRecent);

        iRecent = (iRecent + 1) % iMaxRecent;
    }

    private void initGUI() {

        Panel pQuery   = new Panel();
        Panel pCommand = new Panel();

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
        butClear   = new Button("Clear");

        butExecute.addActionListener(this);
        butClear.addActionListener(this);
        pCommand.add("East", butExecute);
        pCommand.add("West", butClear);
        pCommand.add("Center", txtCommand);

        gResult = new Grid();

        setLayout(new BorderLayout());
        pResult.add("Center", gResult);
        pQuery.add("North", pCommand);
        pQuery.add("Center", pResult);
        fMain.add("Center", pQuery);

        tTree = new Tree();

        // (ulrivo): screen with less than 640 width
        Dimension d = Toolkit.getDefaultToolkit().getScreenSize();

        if (d.width >= 640) {
            tTree.setMinimumSize(new Dimension(200, 100));
        } else {
            tTree.setMinimumSize(new Dimension(80, 100));
        }

        gResult.setMinimumSize(new Dimension(200, 300));
        fMain.add("West", tTree);
        doLayout();
        fMain.pack();
    }

    protected void refreshTree() {

        boolean wasAutoCommit = false;

        tTree.removeAll();

        try {
            wasAutoCommit = cConn.getAutoCommit();

            cConn.setAutoCommit(false);

            int color_table  = Color.yellow.getRGB();
            int color_column = Color.orange.getRGB();
            int color_index  = Color.red.getRGB();

            tTree.addRow("", dMeta.getURL(), "-", 0);

            String[] usertables = {
                "TABLE", "GLOBAL TEMPORARY", "VIEW"
            };

            // fredt@users Schema support
            Vector schemas = new Vector();
            Vector tables  = new Vector();

            // sqlbob@users Added remarks.
            Vector    remarks = new Vector();
            ResultSet result  = dMeta.getTables(null, null, null, usertables);

            try {
                while (result.next()) {
                    schemas.addElement(result.getString(2));
                    tables.addElement(result.getString(3));
                    remarks.addElement(result.getString(5));
                }
            } finally {
                result.close();
            }

            for (int i = 0; i < tables.size(); i++) {
                String name   = (String) tables.elementAt(i);
                String schema = (String) schemas.elementAt(i);
                String key    = "tab-" + name + "-";

                tTree.addRow(key, name, "+", color_table);

                // sqlbob@users Added remarks.
                String remark = (String) remarks.elementAt(i);

                if ((schema != null) && !schema.trim().equals("")) {
                    tTree.addRow(key + "s", "schema: " + schema);
                }

                if ((remark != null) && !remark.trim().equals("")) {
                    tTree.addRow(key + "r", " " + remark);
                }

                ResultSet col = dMeta.getColumns(null, schema, name, null);

                try {
                    while (col.next()) {
                        String c  = col.getString(4);
                        String k1 = key + "col-" + c + "-";

                        tTree.addRow(k1, c, "+", color_column);

                        String type = col.getString(6);

                        tTree.addRow(k1 + "t", "Type: " + type);

                        boolean nullable = col.getInt(11)
                                           != DatabaseMetaData.columnNoNulls;

                        tTree.addRow(k1 + "n", "Nullable: " + nullable);
                    }
                } finally {
                    col.close();
                }

                tTree.addRow(key + "ind", "Indices", "+", 0);

                ResultSet ind = dMeta.getIndexInfo(null, schema, name, false,
                                                   false);
                String oldiname = null;

                try {
                    while (ind.next()) {
                        boolean nonunique = ind.getBoolean(4);
                        String  iname     = ind.getString(6);
                        String  k2        = key + "ind-" + iname + "-";

                        if ((oldiname == null || !oldiname.equals(iname))) {
                            tTree.addRow(k2, iname, "+", color_index);
                            tTree.addRow(k2 + "u", "Unique: " + !nonunique);

                            oldiname = iname;
                        }

                        String c = ind.getString(9);

                        tTree.addRow(k2 + "c-" + c + "-", c);
                    }
                } finally {
                    ind.close();
                }
            }

            tTree.addRow("p", "Properties", "+", 0);
            tTree.addRow("pu", "User: " + dMeta.getUserName());
            tTree.addRow("pr", "ReadOnly: " + cConn.isReadOnly());
            tTree.addRow("pa", "AutoCommit: " + cConn.getAutoCommit());
            tTree.addRow("pd", "Driver: " + dMeta.getDriverName());
            tTree.addRow("pp", "Product: " + dMeta.getDatabaseProductName());
            tTree.addRow("pv",
                         "Version: " + dMeta.getDatabaseProductVersion());
        } catch (SQLException e) {
            tTree.addRow("", "Error getting metadata:", "-", 0);
            tTree.addRow("-", e.getMessage());
            tTree.addRow("-", e.getSQLState());
        } finally {
            try {
                cConn.setAutoCommit(wasAutoCommit);
            } catch (SQLException e) {}
        }

        tTree.update();
    }
}
