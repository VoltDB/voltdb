/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTOREVALUEVECTOR_H
#define HSTOREVALUEVECTOR_H

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <cassert>
#include "boost/unordered_map.hpp"
#include "boost/array.hpp"
#include "common/types.h"
#include "common/NValue.hpp"

namespace voltdb {

/**
 * Fixed size Array of Values. Less flexible but faster than std::vector of Value.
 * The constructor initializes all data with zeros, resulting in type==INVALID.
 * Destructors of the Value objects inside of this class will not be called.
 * Just the data area is revoked, which
 * means Value should not rely on destructor.
 */
template <typename V> class GenericValueArray {
public:
    inline GenericValueArray() : size_(0),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        ::memset(data_, 0, sizeof(V) * size_);
    }
    inline GenericValueArray(int size) : size_(size),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        ::memset(data_, 0, sizeof(V) * size_);
    }
    inline GenericValueArray(const GenericValueArray &rhs) : size_(rhs.size_),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        //::memset(data_, 0, sizeof(V) * size_);
        /*for (int i = 0; i < size_; ++i) {
            data_[i] = rhs.data_[i];
        }*/
        ::memcpy(data_, rhs.data_, sizeof(V) * size_);
    }
    inline ~GenericValueArray() {
        delete[] reinterpret_cast<char*>(data_);
    }

    inline void reset(int size) {
        delete[] reinterpret_cast<char*>(data_);
        size_ = size;
        data_ = reinterpret_cast<V*>(new char[sizeof(V) * size_]);
        ::memset(data_, 0, sizeof(V) * size_);
    }

    GenericValueArray<V>& operator=(const GenericValueArray<V> &rhs);

    inline const V* getRawPointer() const { return data_; }
    inline V& operator[](int index) {
        assert (index >= 0);
        assert (index < size_);
        return data_[index];
    }
    inline const V& operator[](int index) const {
        assert (index >= 0);
        assert (index < size_);
        return data_[index];
    }
    inline int size() const { return size_;}
    inline int compareValue (const GenericValueArray &rhs) const {
        assert (size_ == rhs.size_);
        for (int i = 0; i < size_; ++i) {
            int ret = data_[i].compare(rhs.data_[i]);
            if (ret != 0) return ret;
        }
        return 0;
    }

    std::string debug() const;
    std::string debug(int columnCount) const;

private:
    int size_;
    V* data_;
};

template <typename V> inline bool operator == (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.compareValue(rhs) == 0;
}
template <typename V> inline bool operator != (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.compareValue(rhs) != 0;
}
template <typename V> inline bool operator < (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.compareValue(rhs) < 0;
}
template <typename V> inline bool operator > (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.compareValue(rhs) > 0;
}
template <typename V> inline bool operator <= (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return !(lhs > rhs);
}
template <typename V> inline bool operator >= (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return !(lhs < rhs);
}
template<> inline GenericValueArray<NValue>::~GenericValueArray() {
    delete[] reinterpret_cast<char*>(data_);
}

template<> inline GenericValueArray<NValue>&
GenericValueArray<NValue>::operator=(const GenericValueArray<NValue> &rhs) {
    delete[] data_;
    size_ = rhs.size_;
    data_ = reinterpret_cast<NValue*>(new char[sizeof(NValue) * size_]);
    ::memcpy(data_, rhs.data_, sizeof(NValue) * size_);
    return *this;
}

template<> inline std::string GenericValueArray<NValue>::debug() const {
    std::string out("[ ");
    for (int i = 0; i < size_; i++) {
        out += data_[i].debug() + " "; // how to do with this...
    }
    return out + "]";
}

template<> inline std::string GenericValueArray<NValue>::debug(int columnCount) const {
    std::string out("[ ");
    for (int i = 0; i < columnCount; i++) {
        out += data_[i].debug() + " "; // how to do with this...
    }
    return out + "]";
}

typedef GenericValueArray<NValue> NValueArray;

/** comparator for NValueArray. */
class NValueArrayComparator {
  public:

    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    NValueArrayComparator(const NValueArrayComparator &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }

    NValueArrayComparator(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }

    NValueArrayComparator(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }

    ~NValueArrayComparator() {
        delete[] column_types_;
    }

    inline bool operator()(const NValueArray& lhs, const NValueArray& rhs) const {
        assert (lhs.size() == rhs.size());
        return lhs.compareValue(rhs) < 0;
    }

  private:
    size_t colCount_;
    ValueType* column_types_;
};

/** comparator for NValueArray. */
template <std::size_t N>
class NValueArrayComparator2 {
  public:
    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    NValueArrayComparator2(const NValueArrayComparator2 &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }

    NValueArrayComparator2(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }

    NValueArrayComparator2(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }

    ~NValueArrayComparator2() {
        delete[] column_types_;
    }

    inline bool operator()(const boost::array<NValue, N> &lhs,
                           const boost::array<NValue, N> &rhs) const
    {
        int cmp = 0;
        for (int i = 0; i < N; ++i) {
            cmp = lhs[i].compareValue(rhs[i], column_types_[i]);
            if (cmp != 0) return (cmp < 0);
        }
        return (cmp < 0);
    }

  private:
    size_t colCount_;
    ValueType* column_types_;
};

/** comparator for NValueArray. */
class NValueArrayEqualityTester {
public:
    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    NValueArrayEqualityTester(const NValueArrayEqualityTester &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }
    NValueArrayEqualityTester(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }
    NValueArrayEqualityTester(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }
    ~NValueArrayEqualityTester() {
        delete[] column_types_;
    }

    inline bool operator()(const NValueArray& lhs, const NValueArray& rhs) const {
        assert (lhs.size() == rhs.size());
        assert (lhs.size() == static_cast<int>(colCount_));
        return lhs.compareValue(rhs) == 0;
    }
private:
    size_t colCount_;
    ValueType* column_types_;
};


}
#endif
