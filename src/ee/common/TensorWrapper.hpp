#ifndef TENSOR_WRAPPER_H
#define TENSOR_WRAPPER_H
#include <iostream>
#include <cassert>
#include <cstring>
#include "common/SQLException.h"
//#include ... Some file to provide a declaration for SQLException.

/**
 *  Wrap a 1D array of doubles to make a 2D
 *  tensor.  Objects of this class do not own
 *  the data pointer.  They just structure it.
 */

class TensorWrapper {
  double     *m_data;        /// The actual data.
  int32_t     m_dataLen;     /// The length of the actual data.
  int32_t     m_numRows;     /// Number of rows of the data representation.
  int32_t     m_numCols;     /// Number of columns of the data reprsentation.
  bool     m_transposed;  /// True if this is transposed.

  TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen, bool transposed) ;
protected:
  double *getData() const;
  int32_t getDataLen() const;
public:
  /**
   * Construct a non-transposed tensor. (need c++11 to get this complie)
   */
  //TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen) :
  //  TensorWrapper(nrows, ncols, data, dataLen, false) {
  //}

  TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen);
  TensorWrapper(char *data, int dataLen);

  /**
   * Construct a transposed tensor from another tensor.
   */

  TensorWrapper transpose(const TensorWrapper &other) ;

  int32_t numRows () const ;

  int32_t numCols() const;

  double get(int32_t row, int32_t col) const;

  void set(int32_t row, int32_t col, double value) ;

private:
  /**
   * This is here just for debugging, to verify
   * that indexes are legal.
   */
  void ensureIndexes(int32_t row, int32_t col) const;
};
#endif /* TENSOR_WRAPPER_H */
