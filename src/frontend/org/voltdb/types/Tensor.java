/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.types;

import java.nio.ByteBuffer;

import org.voltdb.planner.PlanningErrorException;

/**
 * This creates a tensor, which is an n-dimensional matrix.
 *
 * We restrict n to 1 or 2 for now.
 *
 * @author Bill White
 *
 */
public class Tensor {
    private static final int M_MAGIC = 0x544E5e52;

    /*
     * This is stored in row major order.  When we serialize it to the EE
     * we will transpose it.
     */
    private double [][] m_data;
    int m_numRows = 1;
    int m_numCols = 1;
    /**
     * Create a 2 dimensional tensor, or a matrix.  The data is
     * stored in row major order.
     *
     * @param data
     * @throws Exception
     */
    public Tensor(double [] [] data) throws Exception {
        assureData(data);
        m_data = data;
    }

    /**
     * Create a 1 dimensional tensor, or a vector.  This is a row
     * vector.
     *
     * @param data
     */
    public Tensor(double [] data) {
        double [][] ndata = new double[][] { data };
        assureData(ndata);
        m_data = ndata;
    }

    /**
     * Create a Tensor with a byte array.
     *
     * @param data
     * @throws IllegalArgumentException
     */
    public Tensor(byte [] data)  throws IllegalArgumentException {
        this(ByteBuffer.wrap(data));
    }

    public Tensor(ByteBuffer buf) throws IllegalArgumentException {
        int magic = buf.getInt();
        if (magic != M_MAGIC) {
            throw new IllegalArgumentException("Bad magic number in Tensor.");
        }
        m_numCols = buf.getInt();
        m_numRows = buf.getInt();
        m_data = new double[m_numRows][];
        for (int idx = 0; idx < m_numRows; idx += 1) {
            m_data[idx] = new double[m_numCols];
        }
        if (buf.remaining() != 8 * (m_numCols * m_numRows)) {
            throw new IllegalArgumentException("Bad buffer size.");
        }
        for (int ridx = 0; ridx < m_numRows; ridx += 1) {
            for (int cidx = 0; cidx < m_numCols; cidx += 1) {
                // Note that we've silently
                // transposed this here, to get back
                // to the original.
                m_data[ridx][cidx] = buf.getDouble();
            }
        }
    }

    public byte[] getCodedVarBinary() throws IllegalArgumentException {
        int size =
                4 // magic
                + 4 // ncolumns
                + 4 // nrows
                + 8 * (m_numRows * m_numCols);
        ByteBuffer buf = ByteBuffer.allocate(size);
        // This is 'TNSR'.
        buf.putInt(M_MAGIC);
        buf.putInt(m_numCols);
        buf.putInt(m_numRows);
        for (int ridx = 0; ridx < m_numRows; ridx += 1) {
            for (int cidx = 0; cidx < m_numCols; cidx += 1) {
                // Note that we've silently
                // transposed this here.
                buf.putDouble(m_data[ridx][cidx]);
            }
        }
        return buf.array();
    }

    private void assureData(double [][] data) throws PlanningErrorException {
        m_numRows = data.length;
        if (data.length == 0) {
            m_numCols = 0;
            return;
        }
        m_numCols = data[0].length;
        for (double [] row : data) {
            if (m_numCols != row.length) {
                throw new IllegalArgumentException("Rows of a matrix are not all the same length.");
            }
        }
    }

    public final int getNumRows() {
        return m_numRows;
    }

    public final int getNumCols() {
        return m_numCols;
    }

    public final double get(int row, int col) throws IllegalArgumentException {
        if ((row < 0) || (m_numRows <= row)) {
            throw new IllegalArgumentException("Bad row index to TensorType.get.");
        }
        if ((col < 0) || (m_numCols <= col)) {
            throw new IllegalArgumentException("Bad column index to TensorType.get.");
        }
        return m_data[row][col];
    }
}
