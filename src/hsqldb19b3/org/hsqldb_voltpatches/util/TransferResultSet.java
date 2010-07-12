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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

/**
 * Helper class for transferring a result set
 *
 * @author Nicolas BAZIN
 * @version 1.7.0
 */
class TransferResultSet {

    Vector   vRows = null;
    int      iRowIdx;
    int      iMaxRowIdx;
    int      iColumnCount;
    String[] sColumnNames = null;
    int[]    iColumnTypes = null;

    TransferResultSet(ResultSet r) {

        iRowIdx      = 0;
        iMaxRowIdx   = 0;
        iColumnCount = 0;
        vRows        = new Vector();

        try {
            while (r.next()) {
                if (sColumnNames == null) {
                    iColumnCount = r.getMetaData().getColumnCount();
                    sColumnNames = new String[iColumnCount + 1];
                    iColumnTypes = new int[iColumnCount + 1];

                    for (int Idx = 0; Idx < iColumnCount; Idx++) {
                        sColumnNames[Idx + 1] =
                            r.getMetaData().getColumnName(Idx + 1);
                        iColumnTypes[Idx + 1] =
                            r.getMetaData().getColumnType(Idx + 1);
                    }

                    vRows.addElement(null);
                }

                iMaxRowIdx++;

                Object[] Values = new Object[iColumnCount + 1];

                for (int Idx = 0; Idx < iColumnCount; Idx++) {
                    Values[Idx + 1] = r.getObject(Idx + 1);
                }

                vRows.addElement(Values);
            }
        } catch (SQLException SQLE) {
            iRowIdx      = 0;
            iMaxRowIdx   = 0;
            iColumnCount = 0;
            vRows        = new Vector();
        }
    }

    TransferResultSet() {

        iRowIdx      = 0;
        iMaxRowIdx   = 0;
        iColumnCount = 0;
        vRows        = new Vector();
    }

    void addRow(String[] Name, int[] type, Object[] Values,
                int nbColumns) throws Exception {

        if ((Name.length != type.length) || (Name.length != Values.length)
                || (Name.length != (nbColumns + 1))) {
            throw new Exception("Size of parameter incoherent");
        }

        if (sColumnNames == null) {
            iColumnCount = nbColumns;
            sColumnNames = Name;
            iColumnTypes = type;

            vRows.addElement(null);
        }

        if ((iMaxRowIdx > 0) && (this.getColumnCount() != nbColumns)) {
            throw new Exception("Wrong number of columns: "
                                + this.getColumnCount()
                                + " column is expected");
        }

        iMaxRowIdx++;

        vRows.addElement(Values);
    }

    boolean next() {

        iRowIdx++;

        return ((iRowIdx <= iMaxRowIdx) && (iMaxRowIdx > 0));
    }

    String getColumnName(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return null;
        }

        return sColumnNames[columnIdx];
    }

    int getColumnCount() {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return 0;
        }

        return iColumnCount;
    }

    int getColumnType(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return 0;
        }

        return iColumnTypes[columnIdx];
    }

    Object getObject(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return null;
        }

        return ((Object[]) vRows.elementAt(iRowIdx))[columnIdx];
    }
}
