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

#include "TensorWrapper.hpp"

#include <sstream>
/**
 *  Wrap a 1D array of doubles to make a 2D
 *  tensor.  Objects of this class do not own
 *  the data pointer.  They just structure it.
 */
  const int M_MAGIC = 0x544E5e52;

  TensorWrapper::TensorWrapper(int32_t nrows,
                               int32_t ncols,
                               const double *data,
                               int dataLen,
                               bool transposed) :
    m_data(data),
    m_dataLen(dataLen),
    m_numRows(transposed ? ncols : nrows),
    m_numCols(transposed ? nrows : ncols),
    m_transposed(transposed) {
    // The data length has to be a multiple
    // of double size, and the total amount of
    // data has to exactly enough to fill the
    // table.
    if ((m_dataLen % sizeof(double) != 0)
        || (m_numRows * m_numCols != m_dataLen/sizeof(double))) {
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Incorrect size for tensor wrapper");
    }
  }

  const double *TensorWrapper::getData() const{
    return m_data;
  }
  int32_t TensorWrapper::getDataLen() const{
    return m_dataLen;
  }

  std::string TensorWrapper::debug() {
      std::ostringstream ostr;
      ostr << "[";
      std::string rsep = "";
      for (int ridx = 0; ridx < m_numRows; ridx += 1) {
          ostr << rsep << "[";
          std::string csep = "";
          for (int cidx = 0; cidx < m_numCols; cidx += 1) {
              ostr << csep << "(" << ridx << ", " << cidx << ") = " << m_data[ridx*m_numRows + cidx];
              csep = ", ";
          }
          ostr << "]";
          rsep = ", ";
      }
      ostr << "]";
      return ostr.str();
  }

  /**
   * Construct a non-transposed tensor. (need c++11 to get this complie)
   */
  //TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen) :
  //  TensorWrapper(nrows, ncols, data, dataLen, false) {
  //}

  TensorWrapper::TensorWrapper(int32_t nrows,
                               int32_t ncols,
                               const double *data,
                               int dataLen):
    m_data(data),
    m_dataLen(dataLen),
    m_numRows(nrows),
    m_numCols(ncols),
    m_transposed(false){
  }

  TensorWrapper TensorWrapper::makeTensorWrapper(const char *data, int dataLen, int32_t numRows, int32_t numCols) {
      int32_t *ptr = reinterpret_cast<int32_t*>(const_cast<char *>(data));
      ptr[0] = M_MAGIC;
      ptr[1] = numRows;
      ptr[2] = numCols;
      return TensorWrapper(data, dataLen);
  }

  TensorWrapper::TensorWrapper(const char *data, int dataLen) :
          m_dataLen(dataLen)
  {

    const int32_t *ip = (const int32_t *)data;
    int magic = (*data + 0);
    if(magic != M_MAGIC)
    {
        fprintf(stderr, "Magic number 0x%08x is wrong.  Should be 0x%08x\n",
                magic, M_MAGIC);
        throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error,
                                   "Unsupported non-VARBINARY type for Matrix function");
    }
    m_numRows = (*data + 1);
    m_numCols = (*data + 2);
    m_data = (const double *)&(ip[3]);
    m_transposed = false;
  }

  /**
   * Construct a transposed tensor from another tensor.
   */

  TensorWrapper TensorWrapper::transpose(const TensorWrapper &other) {
    return TensorWrapper(other.numRows(), other.numCols(),
                         other.getData(),
                         other.getDataLen(),
                         ! m_transposed);
  }

  int32_t TensorWrapper::numRows () const {
    if (m_transposed) {
      return m_numCols;
    }
    return m_numRows;
  }

  int32_t TensorWrapper::numCols() const{
    if (m_transposed) {
      return m_numRows;
    }
    return m_numCols;
  }

  double TensorWrapper::get(int32_t row, int32_t col) const{
    ensureIndexes(row, col);
    if (m_transposed) {
      return m_data[row*m_numCols + col];
    }
    return m_data[row*m_numCols + col];
  }

  void TensorWrapper::set(int32_t row,
                          int32_t col,
                          double value) {
    ensureIndexes(row, col);
    if (m_transposed) {
      const_cast<double *>(m_data)[row*m_numCols + col] = value;
    } else {
      const_cast<double *>(m_data)[col*m_numRows + row] = value;
    }
  }

  /**
   * This is here just for debugging, to verify
   * that indexes are legal.
   */
  void TensorWrapper::ensureIndexes(int32_t row, int32_t col) const{
    // Swap row and column if transposed.
    if (m_transposed) {
      int32_t tmp = row;
      row = col;
      col = tmp;
    }
    if ((row < 0) || (m_numRows <= row)) {
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Bad row index for creating a tensor");
    }
    if ((col < 0) || (m_numCols <= col)) {
      // You will have to work out the
      // parameters to SQLException here.
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Bad column index creating a tensor.");
      //std::cout<<"SQLException"<<std::endl;
    }
  }
