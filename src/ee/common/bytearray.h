/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#ifndef HSTOREBYTEARRAY_H
#define HSTOREBYTEARRAY_H

#include <common/debuglog.h>
#include <cstring>
#include <boost/shared_array.hpp>

namespace voltdb {

/**
 * A safe and handy char* container.
 * std::string is a good container, but we have to be very careful
 * for a binary string that might include '\0' in arbitrary position
 * because of its implicit construction from const char*.
 * std::vector<char> or boost::array<char, N> works, but we can't pass
 * the objects without copying the elements, which has significant overhead.
 *
 * I made this class to provide same semantics as Java's "byte[]"
 * so that this class guarantees following properties.
 *
 * 1. ByteArray is always safe against '\0'.
 *  This class has no method that implicitly accepts std::string which can
 *  be automatically constructed from NULL-terminated const char*. Be careless!
 * 2. ByteArray has explicit "length" property.
 *  This is what boost::shared_array<char> can't provide.
 * 3. Passing ByteArray (not ByteArray* nor ByteArray&) has almost no cost.
 *  This is what boost::array<char, N> and std::vector<char> can't provide,
 *  which copies elements everytime.
 *  Copy constructor of this class just copies an internal smart pointer.
 *  You can pass around instances of this class just like smart pointer/iterator.
 * 4. No memory leaks.
 * 5. All methods are exception-safe. Nothing dangerouns happens even if Outofmemory happens.
 */
template <typename T> class GenericArray {
public:
    // corresponds to "byte[] bar = null;" in Java
    GenericArray() { reset(); };

    // corresponds to "byte[] bar = new byte[len];" in Java
    // explicit because ByteArray bar = 10; sounds really weird in the semantics.
    explicit GenericArray(int length) { resetAndExpand(length); };

    // corresponds to "byte[] bar = new byte[] {1,2,...,10};" in Java
    // this constructor is safe because it explicitly receives length.
    GenericArray(const T* data, int length) {
        resetAndExpand(length);
        assign(data, 0, length);
    };

    // IMPORTANT : NEVER make a constructor that accepts std::string! It
    // demolishes all the significance of this class.

    // corresponds to "byte[] bar = bar2;" in Java. Note that this has no cost.
    GenericArray(const GenericArray<T> &rhs) {
        data_ = rhs.data_;
        length_ = rhs.length_;
    };
    inline GenericArray<T>& operator=(const GenericArray<T> &rhs) {
        data_ = rhs.data_;
        length_ = rhs.length_;
        return *this;
    }

    ~GenericArray() {};

    // corresponds to "(bar == null)" in Java
    bool isNull() const { return data_ == NULL; };
    // corresponds to "bar = null;" in Java
    void reset() {
        data_.reset();
        length_ = -1;
    };
    // corresponds to "bar = new byte[len];" in Java
    void resetAndExpand(int newLength) {
        vassert(newLength >= 0);
        data_ = boost::shared_array<T>(new T[newLength]);
        ::memset(data_.get(), 0, newLength * sizeof(T));
        length_ = newLength;
    };
    // corresponds to "tmp = new byte[newlen]; System.arraycopy(bar to tmp); bar = tmp;" in Java
    void copyAndExpand(int newLength) {
        vassert(newLength >= 0);
        vassert(newLength > length_);
        boost::shared_array<T> newData(new T[newLength]);
        ::memset(newData.get(), 0, newLength * sizeof(T)); // makes valgrind happy.
        ::memcpy(newData.get(), data_.get(), length_ * sizeof(T));
        data_ = newData;
        length_ = newLength;
    };
    // corresponds to "(bar.length)" in Java
    int length() const { return length_; };
    const T* data() const { return data_.get(); };
    T* data() { return data_.get(); };


    // helper functions for convenience.
    void assign(const T* assignedData, int offset, int assignedLength) {
        vassert(!isNull());
        vassert(length_ >= offset + assignedLength);
        vassert(offset >= 0);
        ::memcpy(data_.get() + offset, assignedData, assignedLength * sizeof(T));
    };
    GenericArray<T> operator+(const GenericArray<T> &tail) const {
        vassert(!isNull());
        vassert(!tail.isNull());
        GenericArray<T> concated(this->length_ + tail.length_);
        concated.assign(this->data_.get(), 0, this->length_);
        concated.assign(tail.data_.get(), this->length_, tail.length_);
        return concated;
    };
    const T& operator[](int index) const {
        vassert(!isNull());
        vassert(length_ > index);
        return data_.get()[index];
    };
    T& operator[](int index) {
        vassert(!isNull());
        vassert(length_ > index);
        return data_.get()[index];
    };
private:
    boost::shared_array<T> data_;
    int length_;
};

typedef GenericArray<char> ByteArray;

}

#endif
