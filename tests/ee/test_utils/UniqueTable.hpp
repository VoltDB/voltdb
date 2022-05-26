/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef TESTS_EE_TEST_UTILS_UNIQUETABLE_HPP
#define TESTS_EE_TEST_UTILS_UNIQUETABLE_HPP

#include <memory>

#include "storage/table.h"

template<class TableType>
struct TableDeleter;

template<class TableType>
class UniqueTable;

/** Instances of voltdb::Table contain a reference count that needs to
    be managed.  Tables also need to be freed for tests to pass in
    valgrind.

    If a table is destroyed before its reference count is decremented,
    confusing error messages can result, which can mask earlier
    errors.  This class addresses both issues, providing a
    unique_ptr-like interface that destroys the table when it goes out
    of scope and also manages the reference count.

    Use makeUniqueTable to create instances of UniqueTable that can
    use methods specific to the voltdb::Table subclass you're dealing
    with.
*/

template<class TableType>
UniqueTable<TableType> makeUniqueTable(TableType* tbl);

template<class TableType>
class UniqueTable {
public:

    UniqueTable()
        : m_table()
    {
    }

    UniqueTable(TableType* tbl)
        : m_table(tbl)
    {
        tbl->incrementRefcount();
    }

    // move constructor
    UniqueTable(UniqueTable&& that) {
        m_table.swap(that.m_table);
    }

    TableType* get() {
        return m_table.get();
    }

    const TableType* get() const {
        return m_table.get();
    }

    TableType& operator*() {
        return m_table.operator*();
    }

    const TableType& operator*() const {
        return m_table.operator*();
    }

    TableType* operator->() {
        return m_table.operator->();
    }

    const TableType* operator->() const {
        return m_table.operator->();
    }

    void reset(TableType* newTable = NULL) {
        m_table.reset(newTable);
        m_table->incrementRefcount();
    }

private:
    std::unique_ptr<TableType, TableDeleter<TableType>> m_table;
};

template<class TableType>
UniqueTable<TableType> makeUniqueTable(TableType* tbl) {
    return UniqueTable<TableType>(tbl);
}

template<class TableType>
struct TableDeleter {
    void operator()(TableType* tbl) const {
        tbl->decrementRefcount();
    }
};

#endif // TESTS_EE_TEST_UTILS_UNIQUETABLE_HPP
