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
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.Hashtable;
import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Choice;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Label;
import java.awt.Panel;
import java.awt.SystemColor;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

// sqlbob@users 20020325 - patch 1.7.0 - enhancements
// sqlbob@users 20020407 - patch 1.7.0 - reengineering
// fredt@users - 20040508 - modified patch by lonbinder@users for saving settings

/**
 * Opens a connection to a database
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.7.2
 * @since Hypersonic SQL
 */
class ConnectionDialog extends Dialog implements ActionListener, ItemListener {

    protected Connection mConnection;
    protected TextField  mName, mDriver, mURL, mUser, mPassword;
    protected Label      mError;
    private String[][]   connTypes;
    private Hashtable    settings;
    private Choice       types, recent;

    /**
     * @throws Exception
     */
    public static Connection createConnection(String driver, String url,
            String user, String password) throws Exception {

        Class.forName(driver).newInstance();

        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Constructor declaration
     *
     *
     * @param owner
     * @param title
     */
    ConnectionDialog(Frame owner, String title) {
        super(owner, title, true);
    }

    private void create() {

        Dimension d = Toolkit.getDefaultToolkit().getScreenSize();

        setLayout(new BorderLayout());

        Panel p = new Panel(new BorderLayout());
        Panel pLabel;
        Panel pText;
        Panel pButton;
        Panel pClearButton;

        // (ulrivo): full size on screen with less than 640 width
        if (d.width >= 640) {
            pLabel       = new Panel(new GridLayout(8, 1, 10, 10));
            pText        = new Panel(new GridLayout(8, 1, 10, 10));
            pButton      = new Panel(new GridLayout(1, 2, 10, 10));
            pClearButton = new Panel(new GridLayout(8, 1, 10, 10));
        } else {
            pLabel       = new Panel(new GridLayout(8, 1));
            pText        = new Panel(new GridLayout(8, 1));
            pButton      = new Panel(new GridLayout(1, 2));
            pClearButton = new Panel(new GridLayout(8, 1));
        }

        p.add("West", pLabel);
        p.add("Center", pText);
        p.add("South", pButton);
        p.add("North", createLabel(""));
        p.add("East", pClearButton);
        p.setBackground(SystemColor.control);
        pText.setBackground(SystemColor.control);
        pLabel.setBackground(SystemColor.control);
        pButton.setBackground(SystemColor.control);
        pLabel.add(createLabel("Recent:"));

        recent = new Choice();

        try {
            settings = ConnectionDialogCommon.loadRecentConnectionSettings();
        } catch (java.io.IOException ioe) {
            ioe.printStackTrace();
        }

        recent.add(ConnectionDialogCommon.emptySettingName);

        Enumeration en = settings.elements();

        while (en.hasMoreElements()) {
            recent.add(((ConnectionSetting) en.nextElement()).getName());
        }

        recent.addItemListener(new ItemListener() {

            public void itemStateChanged(ItemEvent e) {

                String s = (String) e.getItem();
                ConnectionSetting setting =
                    (ConnectionSetting) settings.get(s);

                if (setting != null) {
                    mName.setText(setting.getName());
                    mDriver.setText(setting.getDriver());
                    mURL.setText(setting.getUrl());
                    mUser.setText(setting.getUser());
                    mPassword.setText(setting.getPassword());
                }
            }
        });
        pText.add(recent);

        Button b;

        b = new Button("Clr");

        b.setActionCommand("Clear");
        b.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {

                ConnectionDialogCommon.deleteRecentConnectionSettings();

                settings = new Hashtable();

                recent.removeAll();
                recent.add(ConnectionDialogCommon.emptySettingName);
                mName.setText(null);
            }
        });
        pClearButton.add(b);
        pLabel.add(createLabel("Setting Name:"));

        mName = new TextField("");

        pText.add(mName);
        pLabel.add(createLabel("Type:"));

        types     = new Choice();
        connTypes = ConnectionDialogCommon.getTypes();

        for (int i = 0; i < connTypes.length; i++) {
            types.add(connTypes[i][0]);
        }

        types.addItemListener(this);
        pText.add(types);
        pLabel.add(createLabel("Driver:"));

        mDriver = new TextField(connTypes[0][1]);

        pText.add(mDriver);
        pLabel.add(createLabel("URL:"));

        mURL = new TextField(connTypes[0][2]);

        mURL.addActionListener(this);
        pText.add(mURL);
        pLabel.add(createLabel("User:"));

        mUser = new TextField("SA");

        mUser.addActionListener(this);
        pText.add(mUser);
        pLabel.add(createLabel("Password:"));

        mPassword = new TextField("");

        mPassword.addActionListener(this);
        mPassword.setEchoChar('*');
        pText.add(mPassword);

        b = new Button("Ok");

        b.setActionCommand("ConnectOk");
        b.addActionListener(this);
        pButton.add(b);

        b = new Button("Cancel");

        b.setActionCommand("ConnectCancel");
        b.addActionListener(this);
        pButton.add(b);
        add("East", createLabel(""));
        add("West", createLabel(""));

        mError = new Label("");

        Panel pMessage = createBorderPanel(mError);

        add("South", pMessage);
        add("North", createLabel(""));
        add("Center", p);
        doLayout();
        pack();

        Dimension size = getSize();

        // (ulrivo): full size on screen with less than 640 width
        if (d.width >= 640) {
            setLocation((d.width - size.width) / 2,
                        (d.height - size.height) / 2);
        } else {
            setLocation(0, 0);
            setSize(d);
        }

        show();
    }

    public static Connection createConnection(Frame owner, String title) {

        ConnectionDialog dialog = new ConnectionDialog(owner, title);

        dialog.create();

        return dialog.mConnection;
    }

    protected static Label createLabel(String s) {

        Label l = new Label(s);

        l.setBackground(SystemColor.control);

        return l;
    }

    protected static Panel createBorderPanel(Component center) {

        Panel p = new Panel();

        p.setBackground(SystemColor.control);
        p.setLayout(new BorderLayout());
        p.add("Center", center);
        p.add("North", createLabel(""));
        p.add("South", createLabel(""));
        p.add("East", createLabel(""));
        p.add("West", createLabel(""));
        p.setBackground(SystemColor.control);

        return p;
    }

    public void actionPerformed(ActionEvent ev) {

        String s = ev.getActionCommand();

        if (s.equals("ConnectOk") || (ev.getSource() instanceof TextField)) {
            try {
                if (mURL.getText().indexOf('\u00AB') >= 0) {
                    throw new Exception("please specify db path");
                }

                mConnection = createConnection(mDriver.getText(),
                                               mURL.getText(),
                                               mUser.getText(),
                                               mPassword.getText());

                if (mName.getText() != null
                        && mName.getText().trim().length() != 0) {
                    ConnectionSetting newSetting =
                        new ConnectionSetting(mName.getText(),
                                              mDriver.getText(),
                                              mURL.getText(), mUser.getText(),
                                              mPassword.getText());

                    ConnectionDialogCommon.addToRecentConnectionSettings(
                        settings, newSetting);
                }

                dispose();
            } catch (java.io.IOException ioe) {
                dispose();
            } catch (Exception e) {
                e.printStackTrace();
                mError.setText(e.toString());
            }
        } else if (s.equals("ConnectCancel")) {
            dispose();
        }
    }

    public void itemStateChanged(ItemEvent e) {

        String s = (String) e.getItem();

        for (int i = 0; i < connTypes.length; i++) {
            if (s.equals(connTypes[i][0])) {
                mDriver.setText(connTypes[i][1]);
                mURL.setText(connTypes[i][2]);
            }
        }
    }
}
