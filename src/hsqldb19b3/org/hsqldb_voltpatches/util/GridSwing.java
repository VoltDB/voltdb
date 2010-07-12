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
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.event.TableModelEvent;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;

// sqlbob@users 20020401 - patch 1.7.0 by sqlbob (RMP) - enhancements
// deccles@users 20040412 - patch 933671 - various bug fixes

/** Simple table model to represent a grid of tuples.
 *
 * New class based on Hypersonic SQL original
 *
 * @author dmarshall@users
 * @version 1.7.2
 * @since 1.7.0
 */
class GridSwing extends AbstractTableModel {

    JTable   jtable = null;
    Object[] headers;
    Vector   rows;

    /**
     * Default constructor.
     */
    public GridSwing() {

        super();

        headers = new Object[0];    // initially empty
        rows    = new Vector();     // initially empty
    }

    /**
     * Get the name for the specified column.
     */
    public String getColumnName(int i) {
        return headers[i].toString();
    }

    public Class getColumnClass(int i) {

        if (rows.size() > 0) {
            Object o = getValueAt(0, i);

            if (o != null) {
                if ((o instanceof java.sql.Timestamp)
                    || (o instanceof java.sql.Time)) {
                    // This is a workaround for JTable's lack of a default
                    // renderer that displays times.
                    // Without this workaround, Timestamps (and similar
                    // classes) will be displayed as dates without times,
                    // since JTable will match these classes to their
                    // java.util.Date superclass.
                    return Object.class;  // renderer will draw .toString().
                }
                return o.getClass();
            }
        }

        return super.getColumnClass(i);
    }

    /**
     * Get the number of columns.
     */
    public int getColumnCount() {
        return headers.length;
    }

    /**
     * Get the number of rows currently in the table.
     */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Get the current column headings.
     */
    public Object[] getHead() {
        return headers;
    }

    /**
     * Get the current table data.
     *  Each row is represented as a <code>String[]</code>
     *  with a single non-null value in the 0-relative
     *  column position.
     *  <p>The first row is at offset 0, the nth row at offset n etc.
     */
    public Vector getData() {
        return rows;
    }

    /**
     * Get the object at the specified cell location.
     */
    public Object getValueAt(int row, int col) {

        if (row >= rows.size()) {
            return null;
        }

        Object[] colArray = (Object[]) rows.elementAt(row);

        if (col >= colArray.length) {
            return null;
        }

        return colArray[col];
    }

    /**
     * Set the name of the column headings.
     */
    public void setHead(Object[] h) {

        headers = new Object[h.length];

        // System.arraycopy(h, 0, headers, 0, h.length);
        for (int i = 0; i < h.length; i++) {
            headers[i] = h[i];
        }
    }

    /**
     * Append a tuple to the end of the table.
     */
    public void addRow(Object[] r) {

        Object[] row = new Object[r.length];

        // System.arraycopy(r, 0, row, 0, r.length);
        for (int i = 0; i < r.length; i++) {
            row[i] = r[i];

            if (row[i] == null) {

//                row[i] = "(null)";
            }
        }

        rows.addElement(row);
    }

    /**
     * Remove data from all cells in the table (without
     *  affecting the current headings).
     */
    public void clear() {
        rows.removeAllElements();
    }

    public void setJTable(JTable table) {
        jtable = table;
    }

    public void fireTableChanged(TableModelEvent e) {
        super.fireTableChanged(e);
        autoSizeTableColumns(jtable);
    }

    public static void autoSizeTableColumns(JTable table) {

        TableModel  model        = table.getModel();
        TableColumn column       = null;
        Component   comp         = null;
        int         headerWidth  = 0;
        int         maxCellWidth = Integer.MIN_VALUE;
        int         cellWidth    = 0;
        TableCellRenderer headerRenderer =
            table.getTableHeader().getDefaultRenderer();

        for (int i = 0; i < table.getColumnCount(); i++) {
            column = table.getColumnModel().getColumn(i);
            comp = headerRenderer.getTableCellRendererComponent(table,
                    column.getHeaderValue(), false, false, 0, 0);
            headerWidth  = comp.getPreferredSize().width + 10;
            maxCellWidth = Integer.MIN_VALUE;

            for (int j = 0; j < Math.min(model.getRowCount(), 30); j++) {
                TableCellRenderer r = table.getCellRenderer(j, i);

                comp = r.getTableCellRendererComponent(table,
                                                       model.getValueAt(j, i),
                                                       false, false, j, i);
                cellWidth = comp.getPreferredSize().width;

                if (cellWidth >= maxCellWidth) {
                    maxCellWidth = cellWidth;
                }
            }

            column.setPreferredWidth(Math.max(headerWidth, maxCellWidth)
                                     + 10);
        }
    }
}
