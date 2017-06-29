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
#ifndef TENSOR_WRAPPER_H
#define TENSOR_WRAPPER_H
#include <iostream>
#include <cassert>
#include <cstring>


#include "common/SQLException.h"

/**
 *  Wrap a 1D array of doubles to make a 2D
 *  tensor.  Objects of this class do not own
 *  the data pointer.  They just structure it.
 */

class TensorWrapper {
  const double     *m_data;        /// The actual data.
  int32_t           m_dataLen;     /// The length of the actual data.
  int32_t           m_numRows;     /// Number of rows of the data representation.
  int32_t           m_numCols;     /// Number of columns of the data reprsentation.
  bool              m_transposed;  /// True if this is transposed.

  TensorWrapper(int32_t nrows,
                int32_t ncols,
                const double *data,
                int dataLen,
                bool transposed) ;
protected:
  const double *getData() const;
  int32_t getDataLen() const;
public:
  /**
   * Construct a non-transposed tensor. (need c++11 to get this complie)
   */
  //TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen) :
  //  TensorWrapper(nrows, ncols, data, dataLen, false) {
  //}

  TensorWrapper(int32_t nrows,
                int32_t ncols,
                const double *data,
                int dataLen);
  TensorWrapper(const char *data, int dataLen);

  /**
   * Construct a transposed tensor from another tensor.
   */

  TensorWrapper transpose(const TensorWrapper &other) ;

  int32_t numRows () const ;

  int32_t numCols() const;

  double get(int32_t row, int32_t col) const;

  void set(int32_t row, int32_t col, double value) ;

  /**
   * Return the number of bytes used by a tensor
   * which has the given number of rows and columns.
   */
  static int32_t tensorByteSize(int32_t numRows, int32_t numCols) {
    return 3 * sizeof(int32_t) + numRows * numCols * sizeof(double);
  }
private:
  /**
   * This is here just for debugging, to verify
   * that indexes are legal.
   */
  void ensureIndexes(int32_t row, int32_t col) const;
};
#endif /* TENSOR_WRAPPER_H */
