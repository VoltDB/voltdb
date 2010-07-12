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
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Event;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Panel;
import java.awt.Scrollbar;
import java.awt.SystemColor;

// sqlbob@users 20020401 - patch 1.7.0 by sqlbob (RMP) - enhancements

/**
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
class Grid extends Panel {

    // drawing
    private Dimension dMinimum;

// boucherb@users changed access for databasemanager2
    protected Font fFont;

// --------------------------------------------------
    private FontMetrics fMetrics;
    private Graphics    gImage;
    private Image       iImage;

    // height / width
    private int iWidth, iHeight;
    private int iRowHeight, iFirstRow;
    private int iGridWidth, iGridHeight;
    private int iX, iY;

    // data
// boucherb@users changed access for databasemanager2
    protected String[] sColHead = new String[0];
    protected Vector   vData    = new Vector();

// --------------------------------------------------
    private int[] iColWidth;
    private int   iColCount;

// boucherb@users changed access for databasemanager2
    protected int iRowCount;

// --------------------------------------------------
    // scrolling
    private Scrollbar sbHoriz, sbVert;
    private int       iSbWidth, iSbHeight;
    private boolean   bDrag;
    private int       iXDrag, iColDrag;

    /**
     * Constructor declaration
     *
     */
    public Grid() {

        super();

        fFont = new Font("Dialog", Font.PLAIN, 12);

        setLayout(null);

        sbHoriz = new Scrollbar(Scrollbar.HORIZONTAL);

        add(sbHoriz);

        sbVert = new Scrollbar(Scrollbar.VERTICAL);

        add(sbVert);
    }

    /**
     * Method declaration
     */
    String[] getHead() {
        return sColHead;
    }

    /**
     * Method declaration
     */
    Vector getData() {
        return vData;
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
    public void setBounds(int x, int y, int w, int h) {

        // fredt@users 20011210 - patch 450412 by elise@users
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
     *
     * @param head
     */
    public void setHead(String[] head) {

        iColCount = head.length;
        sColHead  = new String[iColCount];
        iColWidth = new int[iColCount];

        for (int i = 0; i < iColCount; i++) {
            sColHead[i]  = head[i];
            iColWidth[i] = 100;
        }

        iRowCount  = 0;
        iRowHeight = 0;
        vData      = new Vector();
    }

    /**
     * Method declaration
     *
     *
     * @param data
     */
    public void addRow(String[] data) {

        if (data.length != iColCount) {
            return;
        }

        String[] row = new String[iColCount];

        for (int i = 0; i < iColCount; i++) {
            row[i] = data[i];

            if (row[i] == null) {
                row[i] = "(null)";
            }
        }

        vData.addElement(row);

        iRowCount++;
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

        if (iRowHeight == 0) {
            return;
        }

        int w = 0;

        for (int i = 0; i < iColCount; i++) {
            w += iColWidth[i];
        }

        iGridWidth  = w;
        iGridHeight = iRowHeight * (iRowCount + 1);

        sbHoriz.setValues(iX, iWidth, 0, iGridWidth);

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
    // to remove this deprecated method we need to rewrite the Grid class as a
    // ScrollPane component
    // sqlbob:  I believe that changing to the JDK1.1 event handler
    // would require browsers to use the Java plugin.
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

        if (g == null) {
            return;
        }

        if (sColHead.length == 0) {
            super.paint(g);

            return;
        }

        if (iWidth <= 0 || iHeight <= 0) {
            return;
        }

        g.setColor(SystemColor.control);
        g.fillRect(iWidth, iHeight, iSbWidth, iSbHeight);

        if (iImage == null) {
            iImage = createImage(iWidth, iHeight);
            gImage = iImage.getGraphics();

            gImage.setFont(fFont);

            if (fMetrics == null) {
                fMetrics = gImage.getFontMetrics();
            }
        }

        if (iRowHeight == 0) {
            iRowHeight = getMaxHeight(fMetrics);

            for (int i = 0; i < iColCount; i++) {
                calcAutoWidth(i);
            }

            adjustScroll();
        }

        gImage.setColor(Color.white);
        gImage.fillRect(0, 0, iWidth, iHeight);
        gImage.setColor(Color.darkGray);
        gImage.drawLine(0, iRowHeight, iWidth, iRowHeight);

        int x = -iX;

        for (int i = 0; i < iColCount; i++) {
            int w = iColWidth[i];

            gImage.setColor(SystemColor.control);
            gImage.fillRect(x + 1, 0, w - 2, iRowHeight);
            gImage.setColor(Color.black);
            gImage.drawString(sColHead[i], x + 2, iRowHeight - 5);
            gImage.setColor(Color.darkGray);
            gImage.drawLine(x + w - 1, 0, x + w - 1, iRowHeight - 1);
            gImage.setColor(Color.white);
            gImage.drawLine(x + w, 0, x + w, iRowHeight - 1);

            x += w;
        }

        gImage.setColor(SystemColor.control);
        gImage.fillRect(0, 0, 1, iRowHeight);
        gImage.fillRect(x + 1, 0, iWidth - x, iRowHeight);
        gImage.drawLine(0, 0, 0, iRowHeight - 1);

        int y = iRowHeight + 1 - iY;
        int j = 0;

        while (y < iRowHeight + 1) {
            j++;

            y += iRowHeight;
        }

        iFirstRow = j;
        y         = iRowHeight + 1;

        for (; y < iHeight && j < iRowCount; j++, y += iRowHeight) {
            x = -iX;

            for (int i = 0; i < iColCount; i++) {
                int   w = iColWidth[i];
                Color b = Color.white,
                      t = Color.black;

                gImage.setColor(b);
                gImage.fillRect(x, y, w - 1, iRowHeight - 1);
                gImage.setColor(t);
                gImage.drawString(getDisplay(i, j), x + 2,
                                  y + iRowHeight - 5);
                gImage.setColor(Color.lightGray);
                gImage.drawLine(x + w - 1, y, x + w - 1, y + iRowHeight - 1);
                gImage.drawLine(x, y + iRowHeight - 1, x + w - 1,
                                y + iRowHeight - 1);

                x += w;
            }

            gImage.setColor(Color.white);
            gImage.fillRect(x, y, iWidth - x, iRowHeight - 1);
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
     *
     *
     * @param e
     * @param x
     * @param y
     */
    public boolean mouseMove(Event e, int x, int y) {

        if (y <= iRowHeight) {
            int xb = x;

            x += iX - iGridWidth;

            int i = iColCount - 1;

            for (; i >= 0; i--) {
                if (x > -7 && x < 7) {
                    break;
                }

                x += iColWidth[i];
            }

            if (i >= 0) {
                if (!bDrag) {
                    setCursor(new Cursor(Cursor.E_RESIZE_CURSOR));

                    bDrag    = true;
                    iXDrag   = xb - iColWidth[i];
                    iColDrag = i;
                }

                return true;
            }
        }

        return mouseExit(e, x, y);
    }

    /**
     * Method declaration
     *
     *
     * @param e
     * @param x
     * @param y
     */
    public boolean mouseDrag(Event e, int x, int y) {

        if (bDrag && x < iWidth) {
            int w = x - iXDrag;

            if (w < 0) {
                w = 0;
            }

            iColWidth[iColDrag] = w;

            adjustScroll();
            repaint();
        }

        return true;
    }

    /**
     * Method declaration
     *
     *
     * @param e
     * @param x
     * @param y
     */
    public boolean mouseExit(Event e, int x, int y) {

        if (bDrag) {
            setCursor(new Cursor(Cursor.DEFAULT_CURSOR));

            bDrag = false;
        }

        return true;
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
     * @param i
     */
    private void calcAutoWidth(int i) {

        int w = 10;

        w = Math.max(w, fMetrics.stringWidth(sColHead[i]));

        for (int j = 0; j < iRowCount; j++) {
            String[] s = (String[]) (vData.elementAt(j));

            w = Math.max(w, fMetrics.stringWidth(s[i]));
        }

        iColWidth[i] = w + 6;
    }

    /**
     * Method declaration
     *
     *
     * @param x
     * @param y
     */
    private String getDisplay(int x, int y) {
        return (((String[]) (vData.elementAt(y)))[x]);
    }

    /**
     * Method declaration
     *
     *
     * @param x
     * @param y
     */
    private String get(int x, int y) {
        return (((String[]) (vData.elementAt(y)))[x]);
    }

    /**
     * Method declaration
     *
     *
     * @param f
     */
    private static int getMaxHeight(FontMetrics f) {
        return f.getHeight() + 4;
    }
}
