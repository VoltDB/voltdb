
#include "TensorWrapper.hpp"
//#include ... Some file to provide a declaration for SQLException.

/**
 *  Wrap a 1D array of doubles to make a 2D
 *  tensor.  Objects of this class do not own
 *  the data pointer.  They just structure it.
 */
  const int M_MAGIC = 0x544E5e52;

  TensorWrapper::TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen, bool transposed) :
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
      // You will have to work out the
      // parameters to SQLException here.
      //std::cout<<"SQLException"<<std::endl;
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Unsupported non-VARBINARY type for Matrix function");
    }
  }

  double *TensorWrapper::getData() const{
    return m_data;
  }
  int32_t TensorWrapper::getDataLen() const{
    return m_dataLen;
  }

  /**
   * Construct a non-transposed tensor. (need c++11 to get this complie)
   */
  //TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen) :
  //  TensorWrapper(nrows, ncols, data, dataLen, false) {
  //}

  TensorWrapper::TensorWrapper(int32_t nrows, int32_t ncols, double *data, int dataLen):
    m_data(data),
    m_dataLen(dataLen),
    m_numRows(nrows),
    m_numCols(ncols),
    m_transposed(false){
  }

  TensorWrapper::TensorWrapper(char *data, int dataLen){

    int32_t *ip = (int32_t *)data;
    int magic = ip[0];
    if(magic != M_MAGIC)
    {
     throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Unsupported non-VARBINARY type for Matrix function");
    }
    m_numRows = ip[1];
    m_numCols = ip[2];
    m_data = (double *)&(ip[3]);

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

  void TensorWrapper::set(int32_t row, int32_t col, double value) {
    ensureIndexes(row, col);
    if (m_transposed) {
      m_data[row*m_numCols + col] = value;
    } else {
      m_data[row*m_numCols + col] = value;
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
      // You will have to work out the
      // parameters to SQLException here.
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Unsupported non-VARBINARY type for Matrix function");
      //std::cout<<"SQLException"<<std::endl;
    }
    if ((col < 0) || (m_numCols <= row)) {
      // You will have to work out the
      // parameters to SQLException here.
      throw voltdb::SQLException(voltdb::SQLException::dynamic_sql_error, "Unsupported non-VARBINARY type for Matrix function");
      //std::cout<<"SQLException"<<std::endl;
    }
  }
