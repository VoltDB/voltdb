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

import java.awt.Dimension;
import java.awt.Image;
import java.awt.Toolkit;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

// sqlbob@users 20020407 - patch 1.7.0 - reengineering
// weconsultants@users 20041109 - patch 1.8.0 - enhancements:
//      Added Methods: setSwingLAF(), LookAndFeelInfo(), setFramePositon()
//      errorMessage(String errorMessage),
//      errorMessage(Exception exceptionMsg,
//      Added: Ability to switch the current LAF while runing (Native,Java or Motif)

/**
 * Common code in the Swing versions of DatabaseManager and Tranfer
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.0
 */
class CommonSwing {

    protected static String messagerHeader = "Database Manager Swing Error";
    protected static String Native         = "Native";
    protected static String Java           = "Java";
    protected static String Motif          = "Motif";
    protected static String plaf           = "plaf";
    protected static String GTK            = "GTK";

    // (ulrivo): An actual Image.
    static Image getIcon(String target) {

        if (target.equalsIgnoreCase("SystemCursor")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Hourglass.gif")).getImage());
        } else if (target.equalsIgnoreCase("Frame")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("hsqldb.gif")).getImage());
        } else if (target.equalsIgnoreCase("Execute")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("run_exc.gif")).getImage());
        } else if (target.equalsIgnoreCase("StatusRunning")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("RedCircle.gif")).getImage());
        } else if (target.equalsIgnoreCase("StatusReady")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("GreenCircle.gif")).getImage());
        } else if (target.equalsIgnoreCase("Clear")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Clear.png")).getImage());
        } else if (target.equalsIgnoreCase("Problem")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("problems.gif")).getImage());
        } else if (target.equalsIgnoreCase("BoldFont")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Bold.gif")).getImage());
        } else if (target.equalsIgnoreCase("ItalicFont")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Italic.gif")).getImage());
        } else if (target.equalsIgnoreCase("ColorSelection")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Colors.png")).getImage());
        } else if (target.equalsIgnoreCase("Close")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Close.png")).getImage());
        } else {
            return (null);
        }
    }

    // (weconsultants@users: Callable errorMessage method
    protected static void errorMessage(String errorMessage) {

        /**
         * Display Jpanel Error messages any text Errors. Overloads
         * errorMessage(Exception exceptionMsg)
         */
        Object[] options = { "OK" };

        JOptionPane.showOptionDialog(null, errorMessage, messagerHeader,
                                     JOptionPane.DEFAULT_OPTION,
                                     JOptionPane.WARNING_MESSAGE, null,
                                     options, options[0]);

        // DatabaseManagerSwing.StatusMessage(READY_STATUS);
    }

    public static void errorMessage(Exception exceptionMsg) {
        errorMessage(exceptionMsg, false);
    }

    // (weconsultants@users: Callable errorMessage method
    public static void errorMessage(Exception exceptionMsg, boolean quiet) {

        /**
         * Display Jpanel Error messages any SQL Errors. Overloads
         * errorMessage(String e)
         */
        Object[] options = { "OK", };

        JOptionPane.showOptionDialog(null, exceptionMsg, messagerHeader,
                                     JOptionPane.DEFAULT_OPTION,
                                     JOptionPane.ERROR_MESSAGE, null,
                                     options, options[0]);

        if (!quiet) {
            exceptionMsg.printStackTrace();
        }

        // DatabaseManagerSwing.StatusMessage(READY_STATUS);
    }

    // (weconsultants@users: Callable setFramePositon method
    static void setFramePositon(JFrame inTargetFrame) {

        Dimension d    = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = inTargetFrame.getSize();

        // (ulrivo): full size on screen with less than 640 width
        if (d.width >= 640) {
            inTargetFrame.setLocation((d.width - size.width) / 2,
                                      (d.height - size.height) / 2);
        } else {
            inTargetFrame.setLocation(0, 0);
            inTargetFrame.setSize(d);
        }
    }

// (weconsultants@users: Commented out, Not need now. Was not being called anyway.. Could delete?
//    static void setDefaultColor() {
//
//        Color hsqlBlue = new Color(102, 153, 204);
//        Color hsqlGreen = new Color(153, 204, 204);
//        UIDefaults d = UIManager.getLookAndFeelDefaults();
//
//        d.put("MenuBar.background", SystemColor.control);
//        d.put("Menu.background", SystemColor.control);
//        d.put("Menu.selectionBackground", hsqlBlue);
//        d.put("MenuItem.background", SystemColor.menu);
//        d.put("MenuItem.selectionBackground", hsqlBlue);
//        d.put("Separator.foreground", SystemColor.controlDkShadow);
//        d.put("Button.background", SystemColor.control);
//        d.put("CheckBox.background", SystemColor.control);
//        d.put("Label.background", SystemColor.control);
//        d.put("Label.foreground", Color.black);
//        d.put("Panel.background", SystemColor.control);
//        d.put("PasswordField.selectionBackground", hsqlGreen);
//        d.put("PasswordField.background", SystemColor.white);
//        d.put("TextArea.selectionBackground", hsqlGreen);
//        d.put("TextField.background", SystemColor.white);
//        d.put("TextField.selectionBackground", hsqlGreen);
//        d.put("TextField.background", SystemColor.white);
//        d.put("ScrollBar.background", SystemColor.controlHighlight);
//        d.put("ScrollBar.foreground", SystemColor.control);
//        d.put("ScrollBar.track", SystemColor.controlHighlight);
//        d.put("ScrollBar.trackHighlight", SystemColor.controlDkShadow);
//        d.put("ScrollBar.thumb", SystemColor.control);
//        d.put("ScrollBar.thumbHighlight", SystemColor.controlHighlight);
//        d.put("ScrollBar.thumbDarkShadow", SystemColor.controlDkShadow);
//        d.put("ScrollBar.thumbLightShadow", SystemColor.controlShadow);
//        d.put("ComboBox.background", SystemColor.control);
//        d.put("ComboBox.selectionBackground", hsqlBlue);
//        d.put("Table.background", SystemColor.white);
//        d.put("Table.selectionBackground", hsqlBlue);
//        d.put("TableHeader.background", SystemColor.control);
//
//        // This doesn't seem to work.
//        d.put("SplitPane.background", SystemColor.control);
//        d.put("Tree.selectionBackground", hsqlBlue);
//        d.put("List.selectionBackground", hsqlBlue);
//    }
    // (weconsultants@users: Callable setSwingLAF method for changing LAF
    static void setSwingLAF(java.awt.Component comp, String targetTheme) {

        try {
            if (targetTheme.equalsIgnoreCase(Native)) {
                UIManager.setLookAndFeel(
                    UIManager.getSystemLookAndFeelClassName());
            } else if (targetTheme.equalsIgnoreCase(Java)) {
                UIManager.setLookAndFeel(
                    UIManager.getCrossPlatformLookAndFeelClassName());
            } else if (targetTheme.equalsIgnoreCase(Motif)) {
                UIManager.setLookAndFeel(
                    "com.sun.java.swing.plaf.motif.MotifLookAndFeel");
            }

//            if (targetTheme.equalsIgnoreCase(plaf)){
//                UIManager.setLookAndFeel("javax.swing.plaf.metal.MetalLookAndFeel");
//            }
//
//            if (targetTheme.equalsIgnoreCase(GTK)){
//                UIManager.setLookAndFeel("com.sun.java.swing.plaf.gtk.GTKLookAndFeel");
//            }
            SwingUtilities.updateComponentTreeUI(comp);

            if (comp instanceof java.awt.Frame) {
                ((java.awt.Frame) comp).pack();
            }
        } catch (Exception e) {
            errorMessage(e);
        }
    }
}
