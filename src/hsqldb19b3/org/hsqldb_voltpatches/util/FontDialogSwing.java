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

import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JColorChooser;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;

//  weconsultants@users 20041109 - original swing port
//  weconsultants@users 20050215 - version 1.8.0 -  Update: Compatbilty fix for JDK 1.3
//      - Replaced: Objects JSpinner spinnerFontSizes and SpinnerNumberModel spinnerModelSizes
//        for JComboBox fontSizesComboBox and String fontSizes[];
public class FontDialogSwing extends JDialog {

    private static boolean      isRunning   = false;
    private static final String BACKGROUND  = "Background";
    private static String       defaultFont = "Dialog";
    private static final String FOREGROUND  = "Foreground";
    private static JButton      bgColorButton;
    private static JCheckBox    ckbbold;
    private static JButton      closeButton;
    private static JButton      fgColorButton;
    private static JComboBox    fontsComboBox;

    //  weconsultants@users 20050215 - Added for Compatbilty fix for JDK 1.3
    private static JComboBox      fontSizesComboBox;
    private static final String[] fontSizes = {
        "8", "9", "10", "11", "12", "13", "14", "16", "18", "24", "36"
    };

    // weconsultants@users 20050215 - Commented out for Compatbilty fix for JDK 1.3
    //  private static JSpinner           spinnerFontSizes;
    //  private static SpinnerNumberModel spinnerModelSizes;
    private static DatabaseManagerSwing fOwner;
    private static JFrame frame =
        new JFrame("DataBaseManagerSwing Font Selection Dialog");
    private static JCheckBox ckbitalic;

    /**
     * Create and display FontDialogSwing Dialog.
     *
     */
    public static void creatFontDialog(DatabaseManagerSwing owner) {

        if (isRunning) {
            frame.setVisible(true);
        } else {
            CommonSwing.setSwingLAF(frame, CommonSwing.Native);

            fOwner = owner;

            frame.setIconImage(CommonSwing.getIcon("Frame"));

            isRunning = true;

            frame.setSize(600, 100);
            CommonSwing.setFramePositon(frame);

            ckbitalic = new JCheckBox(
                new ImageIcon(CommonSwing.getIcon("ItalicFont")));

            ckbitalic.putClientProperty("is3DEnabled", Boolean.TRUE);
            ckbitalic.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    setStyle();
                }
            });

            ckbbold =
                new JCheckBox(new ImageIcon(CommonSwing.getIcon("BoldFont")));

            ckbbold.putClientProperty("is3DEnabled", Boolean.TRUE);
            ckbbold.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    setStyle();
                }
            });

            fgColorButton = new JButton(
                "Foreground",
                new ImageIcon(CommonSwing.getIcon("ColorSelection")));

            fgColorButton.putClientProperty("is3DEnabled", Boolean.TRUE);
            fgColorButton.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    setColor(FOREGROUND);
                }
            });

            bgColorButton = new JButton(
                "Background",
                new ImageIcon(CommonSwing.getIcon("ColorSelection")));

            bgColorButton.putClientProperty("is3DEnabled", Boolean.TRUE);
            bgColorButton.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    setColor(BACKGROUND);
                }
            });

            closeButton =
                new JButton("Close",
                            new ImageIcon(CommonSwing.getIcon("Close")));

            closeButton.putClientProperty("is3DEnabled", Boolean.TRUE);
            closeButton.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    frame.setVisible(false);
                }
            });

            GraphicsEnvironment ge =
                GraphicsEnvironment.getLocalGraphicsEnvironment();
            String[]  fontNames = ge.getAvailableFontFamilyNames();
            Dimension fontsComboBoxDimension = new Dimension(160, 25);

            fontsComboBox = new JComboBox(fontNames);

            fontsComboBox.putClientProperty("is3DEnabled", Boolean.TRUE);
            fontsComboBox.setMaximumSize(fontsComboBoxDimension);
            fontsComboBox.setPreferredSize(fontsComboBoxDimension);
            fontsComboBox.setMaximumSize(fontsComboBoxDimension);
            fontsComboBox.setEditable(false);
            fontsComboBox.setSelectedItem(defaultFont);
            fontsComboBox.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent e) {
                    setFont();
                }
            });

            // weconsultants@users 20050215 - Added for Compatbilty fix for  JDK 1.3
            fontSizesComboBox = new JComboBox(fontSizes);

            Dimension spinnerDimension = new Dimension(45, 25);

            fontSizesComboBox.putClientProperty("is3DEnabled", Boolean.TRUE);
            fontSizesComboBox.setMinimumSize(spinnerDimension);
            fontSizesComboBox.setPreferredSize(spinnerDimension);
            fontSizesComboBox.setMaximumSize(spinnerDimension);
            fontSizesComboBox.addItemListener(new ItemListener() {

                public void itemStateChanged(ItemEvent evt) {

                    if (evt.getStateChange() == ItemEvent.SELECTED) {
                        setFontSize((String) evt.getItem());
                    }
                }
            });

            // weconsultants@users 20050215 - Commented out for Compatbilty fix for  JDK 1.3
            //            Dimension spinnerDimension = new Dimension(50, 25);
            //            spinnerFontSizes = new JSpinner();
            //            spinnerFontSizes.putClientProperty("is3DEnabled", Boolean.TRUE);
            //            spinnerFontSizes.setMinimumSize(spinnerDimension);
            //            spinnerFontSizes.setPreferredSize(spinnerDimension);
            //            spinnerFontSizes.setMaximumSize(spinnerDimension);
            //            spinnerModelSizes = new SpinnerNumberModel(12, 8, 72, 1);
            //            spinnerFontSizes.setModel(spinnerModelSizes);
            //            spinnerFontSizes.addChangeListener(new ChangeListener() {
            //                public void stateChanged(ChangeEvent e) {
            //                    setFontSize();
            //                }
            //            });
            Container contentPane = frame.getContentPane();

            contentPane.setLayout(new FlowLayout());
            contentPane.add(fontsComboBox);

            // weconsultants@users 20050215 - Commented out for Compatbilty fix for 1.3
            // contentPane.add(spinnerFontSizes);
            // weconsultants@users 20050215 - Added for Compatbilty fix for 1.3
            contentPane.add(fontSizesComboBox);
            contentPane.add(ckbbold);
            contentPane.add(ckbitalic);
            contentPane.add(fgColorButton);
            contentPane.add(bgColorButton);
            contentPane.add(closeButton);
            frame.pack();
            frame.setVisible(false);
        }
    }

    public static void setFont() {

        Font txtResultFont = fOwner.txtResult.getFont();

        fOwner.txtResult.setFont(
            new Font(
                fontsComboBox.getSelectedItem().toString(),
                txtResultFont.getStyle(), txtResultFont.getSize()));

        Font txtCommandFont = fOwner.txtResult.getFont();

        fOwner.txtCommand.setFont(
            new Font(
                fontsComboBox.getSelectedItem().toString(),
                txtCommandFont.getStyle(), txtCommandFont.getSize()));

        Font txtTreeFont = fOwner.txtResult.getFont();

        fOwner.tTree.setFont(
            new Font(
                fontsComboBox.getSelectedItem().toString(),
                txtTreeFont.getStyle(), txtTreeFont.getSize()));
    }

    /**
     * Displays a color chooser and Sets the selected color.
     */
    public static void setFontSize(String inFontSize) {

        // weconsultants@users 20050215 - Changed for Compatbilty fix for JDK 1.3
        // Convert Strng to float for deriveFont() call
        Float stageFloat = new Float(inFontSize);
        float fontSize   = stageFloat.floatValue();
        Font  fonttTree  = fOwner.tTree.getFont().deriveFont(fontSize);

        fOwner.tTree.setFont(fonttTree);

        Font fontTxtCommand =
            fOwner.txtCommand.getFont().deriveFont(fontSize);

        fOwner.txtCommand.setFont(fontTxtCommand);

        Font fontTxtResult = fOwner.txtResult.getFont().deriveFont(fontSize);

        fOwner.txtResult.setFont(fontTxtResult);
    }

    /**
     * Changes the style (Bold, Italic ) of the selected text by checking the
     * style buttons
     */
    public static void setStyle() {

        int style = Font.PLAIN;

        if (ckbbold.isSelected()) {
            style |= Font.BOLD;
        }

        if (ckbitalic.isSelected()) {
            style |= Font.ITALIC;
        }

        fOwner.tTree.setFont(fOwner.txtCommand.getFont().deriveFont(style));
        fOwner.txtCommand.setFont(
            fOwner.txtCommand.getFont().deriveFont(style));
        fOwner.txtResult.setFont(
            fOwner.txtResult.getFont().deriveFont(style));
    }

    public static void setColor(String inTarget) {

        if (inTarget.equals(BACKGROUND)) {
            Color backgroundColor = JColorChooser.showDialog(null,
                "DataBaseManagerSwing Choose Background Color",
                fOwner.txtResult.getBackground());

            if (backgroundColor != null) {
                bgColorButton.setBackground(backgroundColor);
                fOwner.txtCommand.setBackground(backgroundColor);
                fOwner.txtResult.setBackground(backgroundColor);
            }
        } else {
            Color foregroundColor = JColorChooser.showDialog(null,
                "DataBaseManagerSwing Choose Foreground Color",
                fOwner.txtResult.getForeground());

            if (foregroundColor != null) {
                fgColorButton.setBackground(foregroundColor);
                fOwner.txtCommand.setForeground(foregroundColor);
                fOwner.txtResult.setForeground(foregroundColor);
            }
        }
    }
}
