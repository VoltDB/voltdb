/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.Vector;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Event;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Panel;
import java.awt.Scrollbar;
import java.awt.SystemColor;
import java.awt.Toolkit;

/**
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.7.0
 * @since Hypersonic SQL
 */
class Tree extends Panel {

    // static
    private static Font        fFont;
    private static FontMetrics fMetrics;
    private static int         iRowHeight;
    private static int         iIndentWidth;
    private int                iMaxTextLength;

    // drawing
    private Dimension dMinimum;
    private Graphics  gImage;
    private Image     iImage;

    // height / width
    private int iWidth, iHeight;
    private int iFirstRow;
    private int iTreeWidth, iTreeHeight;
    private int iX, iY;

    // data
    private Vector vData;
    private int    iRowCount;

    // scrolling
    private Scrollbar sbHoriz, sbVert;
    private int       iSbWidth, iSbHeight;

    static {
        fFont        = new Font("Dialog", Font.PLAIN, 12);
        fMetrics     = Toolkit.getDefaultToolkit().getFontMetrics(fFont);
        iRowHeight   = getMaxHeight(fMetrics);
        iIndentWidth = 12;
    }

    /**
     * Constructor declaration
     *
     */
    Tree() {

        super();

        vData = new Vector();

        setLayout(null);

        sbHoriz = new Scrollbar(Scrollbar.HORIZONTAL);

        add(sbHoriz);

        sbVert = new Scrollbar(Scrollbar.VERTICAL);

        add(sbVert);
    }

    /**
     * Method declaration
     *
     *
     * @param d
     */
    public void setMinimumSize(Dimension d) {
        dMinimum = d;
    }

    /**
     * Method declaration
     *
     *
     * @param x
     * @param y
     * @param w
     * @param h
     */

// fredt@users 20011210 - patch 450412 by elise@users
// with additional replacement of deprecated methods
    public void setBounds(int x, int y, int w, int h) {

        super.setBounds(x, y, w, h);

        iSbHeight = sbHoriz.getPreferredSize().height;
        iSbWidth  = sbVert.getPreferredSize().width;
        iHeight   = h - iSbHeight;
        iWidth    = w - iSbWidth;

        sbHoriz.setBounds(0, iHeight, iWidth, iSbHeight);
        sbVert.setBounds(iWidth, 0, iSbWidth, iHeight);
        adjustScroll();

        iImage = null;

        repaint();
    }

    /**
     * Method declaration
     *
     */
    public void removeAll() {

        vData     = new Vector();
        iRowCount = 0;

        adjustScroll();

        iMaxTextLength = 10;

        repaint();
    }

    /**
     * Method declaration
     *
     *
     * @param key
     * @param value
     * @param state
     * @param color
     */
    public void addRow(String key, String value, String state, int color) {

        String[] row = new String[4];

        if (value == null) {
            value = "";
        }

        row[0] = key;
        row[1] = value;
        row[2] = state;    // null / "-" / "+"
        row[3] = String.valueOf(color);

        vData.addElement(row);

        int len = fMetrics.stringWidth(value);

        if (len > iMaxTextLength) {
            iMaxTextLength = len;
        }

        iRowCount++;
    }

    /**
     * Method declaration
     *
     *
     * @param key
     * @param value
     */
    public void addRow(String key, String value) {
        addRow(key, value, null, 0);
    }

    /**
     * Method declaration
     *
     */
    public void update() {
        adjustScroll();
        repaint();
    }

    /**
     * Method declaration
     *
     */
    void adjustScroll() {

        iTreeHeight = iRowHeight * (iRowCount + 1);

        // correct would be iMaxTextLength + iMaxIndent*iIndentWidth
        iTreeWidth = iMaxTextLength * 2;

        sbHoriz.setValues(iX, iWidth, 0, iTreeWidth);

        int v = iY / iRowHeight,
            h = iHeight / iRowHeight;

        sbVert.setValues(v, h, 0, iRowCount + 1);

        iX = sbHoriz.getValue();
        iY = iRowHeight * sbVert.getValue();
    }

    /**
     * Method declaration
     *
     *
     * @param e
     */

// fredt@users 20020130 - comment by fredt
// to remove this deprecated method we need to rewrite the Tree class as a
// ScrollPane component
    public boolean handleEvent(Event e) {

        switch (e.id) {

            case Event.SCROLL_LINE_UP :
            case Event.SCROLL_LINE_DOWN :
            case Event.SCROLL_PAGE_UP :
            case Event.SCROLL_PAGE_DOWN :
            case Event.SCROLL_ABSOLUTE :
                iX = sbHoriz.getValue();
                iY = iRowHeight * sbVert.getValue();

                repaint();

                return true;
        }

        return super.handleEvent(e);
    }

    /**
     * Method declaration
     *
     *
     * @param g
     */
    public void paint(Graphics g) {

        if (g == null || iWidth <= 0 || iHeight <= 0) {
            return;
        }

        g.setColor(SystemColor.control);
        g.fillRect(iWidth, iHeight, iSbWidth, iSbHeight);

        if (iImage == null) {
            iImage = createImage(iWidth, iHeight);
            gImage = iImage.getGraphics();

            gImage.setFont(fFont);
        }

        gImage.setColor(Color.white);
        gImage.fillRect(0, 0, iWidth, iHeight);

        int[]    lasty = new int[100];
        String[] root  = new String[100];

        root[0] = "";

        int currentindent = 0;
        int y             = iRowHeight;

        y -= iY;

        boolean closed = false;

        for (int i = 0; i < iRowCount; i++) {
            String[] s      = (String[]) vData.elementAt(i);
            String   key    = s[0];
            String   data   = s[1];
            String   folder = s[2];
            int      ci     = currentindent;

            for (; ci > 0; ci--) {
                if (key.startsWith(root[ci])) {
                    break;
                }
            }

            if (root[ci].length() < key.length()) {
                ci++;
            }

            if (closed && ci > currentindent) {
                continue;
            }

            closed   = folder != null && folder.equals("+");
            root[ci] = key;

            int x = iIndentWidth * ci - iX;

            gImage.setColor(Color.lightGray);
            gImage.drawLine(x, y, x + iIndentWidth, y);
            gImage.drawLine(x, y, x, lasty[ci]);

            lasty[ci + 1] = y;

            int py = y + iRowHeight / 3;
            int px = x + iIndentWidth * 2;

            if (folder != null) {
                lasty[ci + 1] += 4;

                int rgb = Integer.parseInt(s[3]);

                gImage.setColor(rgb == 0 ? Color.white
                                         : new Color(rgb));
                gImage.fillRect(x + iIndentWidth - 3, y - 3, 7, 7);
                gImage.setColor(Color.black);
                gImage.drawRect(x + iIndentWidth - 4, y - 4, 8, 8);
                gImage.drawLine(x + iIndentWidth - 2, y,
                                x + iIndentWidth + 2, y);

                if (folder.equals("+")) {
                    gImage.drawLine(x + iIndentWidth, y - 2,
                                    x + iIndentWidth, y + 2);
                }
            } else {
                px -= iIndentWidth;
            }

            gImage.setColor(Color.black);
            gImage.drawString(data, px, py);

            currentindent = ci;
            y             += iRowHeight;
        }

        g.drawImage(iImage, 0, 0, this);
    }

    /**
     * Method declaration
     *
     *
     * @param g
     */
    public void update(Graphics g) {
        paint(g);
    }

    /**
     * Method declaration
     */
    public Dimension preferredSize() {
        return dMinimum;
    }

    /**
     * Method declaration
     */
    public Dimension getPreferredSize() {
        return dMinimum;
    }

    /**
     * Method declaration
     */
    public Dimension getMinimumSize() {
        return dMinimum;
    }

    /**
     * Method declaration
     */
    public Dimension minimumSize() {
        return dMinimum;
    }

    /**
     * Method declaration
     *
     *
     * @param e
     * @param x
     * @param y
     */
    public boolean mouseDown(Event e, int x, int y) {

        if (iRowHeight == 0 || x > iWidth || y > iHeight) {
            return true;
        }

        y += iRowHeight / 2;

        String[] root = new String[100];

        root[0] = "";

        int     currentindent = 0;
        int     cy            = iRowHeight;
        boolean closed        = false;
        int     i             = 0;

        y += iY;

        for (; i < iRowCount; i++) {
            String[] s      = (String[]) vData.elementAt(i);
            String   key    = s[0];
            String   folder = s[2];
            int      ci     = currentindent;

            for (; ci > 0; ci--) {
                if (key.startsWith(root[ci])) {
                    break;
                }
            }

            if (root[ci].length() < key.length()) {
                ci++;
            }

            if (closed && ci > currentindent) {
                continue;
            }

            if (cy <= y && cy + iRowHeight > y) {
                break;
            }

            root[ci]      = key;
            closed        = folder != null && folder.equals("+");
            currentindent = ci;
            cy            += iRowHeight;
        }

        if (i >= 0 && i < iRowCount) {
            String[] s      = (String[]) vData.elementAt(i);
            String   folder = s[2];

            if (folder != null && folder.equals("+")) {
                folder = "-";
            } else if (folder != null && folder.equals("-")) {
                folder = "+";
            }

            s[2] = folder;

            vData.setElementAt(s, i);
            repaint();
        }

        return true;
    }

    /**
     * Method declaration
     *
     *
     * @param f
     */
    private static int getMaxHeight(FontMetrics f) {
        return f.getHeight() + 2;
    }
}
