/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#ifndef HSTORETABLEINDEX_H
#define HSTORETABLEINDEX_H

#include <vector>
#include <string>
#include "boost/shared_ptr.hpp"
#include "boost/tuple/tuple.hpp"
#include "common/ids.h"
#include "common/types.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "indexes/IndexStats.h"
#include "common/ThreadLocalPool.h"

namespace voltdb {

/**
 * Parameter for constructing TableIndex. TupleSchema, then key schema
 */
struct TableIndexScheme {
    TableIndexScheme() {
        tupleSchema = keySchema = NULL;
    }
    TableIndexScheme(std::string name, TableIndexType type, std::vector<int32_t> columnIndices,
                     std::vector<ValueType> columnTypes, bool unique, bool intsOnly,
                     TupleSchema *tupleSchema) {
        this->name = name; this->type = type; this->columnIndices = columnIndices;
        this->columnTypes = columnTypes; this->unique = unique; this->intsOnly = intsOnly;
        this->tupleSchema = tupleSchema; this->keySchema = NULL;
    }

    std::string name;
    TableIndexType type;
    std::vector<int32_t> columnIndices;
    std::vector<ValueType> columnTypes;
    bool unique;
    bool intsOnly;
    TupleSchema *tupleSchema;
    TupleSchema *keySchema;

public:
    void setTree() {
        type = BALANCED_TREE_INDEX;
    }
    void setHash() {
        type = HASH_TABLE_INDEX;
    }
};

/**
 * voltdb::TableIndex class represents a secondary index on a table which
 * is currently implemented as a binary tree (std::map) mapping from key value
 * to tuple pointers. This might involve overhead because of memory
 * fragmentation and pointer tracking on runtime, so we might shift to B+Tree
 * later.
 *
 * TableIndex receives a whole TableTuple to be added/deleted/replaced.
 * PersistentTable passes the TableTuple in TableTuple or in UndoLog to
 * TableIndex for changing/reverting entries in the index. TableIndex gets
 * a subset of the TableTuple only for columns in the index, so there are
 * two types of TableTuple objects used in different meaning. See method
 * comments to check which the method needs to be passed.
 *
 * TableIndex may or may not be a Unique Index. If the index is a
 * unique index, PersistentTable checks uniqueness of
 * inserted/replaced values.
 *
 * TableIndex is an abstract class without any
 * implementation. Implementation class is specialized to uniqueness,
 * column types and numbers for higher performance.
 *
 * See IntsUniqueIndex, IntsMultimapIndex, GenericUniqueIndex,
 * GenericMultimapIndex and ArrayUniqueIndex.
 *
 * @see TableIndexFactory
 */
class TableIndex
{
    friend class TableIndexFactory;

public:
    virtual ~TableIndex();

    /**
     * adds passed value as an index entry linked to given tuple
     */
    virtual bool addEntry(const TableTuple *tuple) = 0;

    /**
     * removes the index entry linked to given value (and tuple
     * pointer, if it's non-unique index).
     */
    virtual bool deleteEntry(const TableTuple *tuple) = 0;

    /**
     * removes the index entry linked to old value and re-link it to new value.
     * The address of the newTupleValue is used as the value in the index (and multimaps) as
     * well as the key for the new entry.
     */
    virtual bool replaceEntry(const TableTuple *oldTupleValue,
                              const TableTuple *newTupleValue) = 0;

    /**
     * Update in place an index entry with a new tuple address
     */
    virtual bool replaceEntryNoKeyChange(const TableTuple *oldTupleValue,
                              const TableTuple *newTupleValue) = 0;

    /**
     * just returns whether the value is already stored. no
     * modification occurs.
     */
    virtual bool exists(const TableTuple* values) = 0;

    /**
     * This method moves to the first tuple equal to given key.  To
     * iterate through all entries with the key (if non-unique index)
     * or all entries that follow the entry, use nextValueAtKey() and
     * advanceToNextKey().
     *
     * This method can be used <b>only for perfect matching</b> in
     * which the whole search key matches with at least one entry in
     * this index.  For example,
     * (a,b,c)=(1,3,2),(1,3,3),(2,1,2),(2,1,3)....
     *
     * This method works for "WHERE a=2 AND b=1 AND c>=2", but does
     * not work for "WHERE a=2 AND b=1 AND c>=1". For partial index
     * search, use moveToKeyOrGreater.
     *
     * @see searchKey the value to be searched. this is NOT tuple
     * data, but chosen values for this index. So, searchKey has to
     * contain values in this index's entry order.
     *
     * @see moveToKeyOrGreater(const TableTuple *)
     * @return true if the value is found. false if not.
     */
    virtual bool moveToKey(const TableTuple *searchKey) = 0;

    /**
     * Find location of the specified tuple in the tuple
     */
    virtual bool moveToTuple(const TableTuple *searchTuple) = 0;

    /**
     * sets the tuple to point the entry found by moveToKey().  calls
     * this repeatedly to get all entries with the search key (for
     * non-unique index).
     *
     * @return true if any entry to return, false if not.
     */
    virtual TableTuple nextValueAtKey() = 0;

    /**
     * sets the tuple to point the entry next to the one found by
     * moveToKey().  calls this repeatedly to get all entries
     * following to the search key (for range query).
     *
     * HOWEVER, this can't be used for partial index search. You can
     * use this only when you in advance know that there is at least
     * one entry that perfectly matches with the search key. In other
     * word, this method SHOULD NOT BE USED in future because there
     * isn't such a case for range query except for cheating case
     * (i.e. TPCC slev which assumes there is always "OID-20" entry).
     *
     * @return true if any entry to return, false if not.
     */
    virtual bool advanceToNextKey()
    {
        throwFatalException("Invoked TableIndex virtual method advanceToNextKey which has no implementation");
    };

    /**
     * This method moves to the first tuple equal or greater than
     * given key.  Use this with nextValue(). This method works for
     * partial index search where following value might not match with
     * any entry in this index.
     *
     * @see searchKey the value to be searched. this is NOT tuple
     *      data, but chosen values for this index.  So, searchKey has
     *      to contain values in this index's entry order.
     */
    virtual void moveToKeyOrGreater(const TableTuple *searchKey)
    {
        throwFatalException("Invoked TableIndex virtual method moveToKeyOrGreater which has no implementation");
    };

    /**
     * This method moves to the first tuple greater than given key.
     * Use this with nextValue().
     *
     * @see searchKey the value to be searched. this is NOT tuple
     *      data, but chosen values for this index.  So, searchKey has
     *      to contain values in this index's entry order.
     */
    virtual void moveToGreaterThanKey(const TableTuple *searchKey)
    {
        throwFatalException("Invoked TableIndex virtual method moveToGreaterThanKey which has no implementation");
    };

    /**
     * This method moves to the beginning or the end of the indexes.
     * Use this with nextValue().
     *
     * @see begin true to move to the beginning, false to the end.
     */
    virtual void moveToEnd(bool begin)
    {
        throwFatalException("Invoked TableIndex virtual method moveToEnd which has no implementation");
    }

    /**
     * sets the tuple to point the entry found by
     * moveToKeyOrGreater().  calls this repeatedly to get all entries
     * with or following to the search key.
     *
     * @return true if any entry to return, false if reached the end
     * of this index.
     */
    virtual TableTuple nextValue()
    {
        throwFatalException("Invoked TableIndex virtual method nextValue which has no implementation");
    };

    /**
     * @return true if lhs is different from rhs in this index, which
     * means replaceEntry has to follow.
     */
    virtual bool checkForIndexChange(const TableTuple *lhs,
                                     const TableTuple *rhs) = 0;

    /**
     * Currently, UniqueIndex is just a TableIndex with additional checks.
     * We might have to make a different class in future for maximizing
     * performance of UniqueIndex.
     */
    inline bool isUniqueIndex() const
    {
        return is_unique_index_;
    }

    virtual size_t getSize() const = 0;

    // Return the amount of memory we think is allocated for this
    // index.
    virtual int64_t getMemoryEstimate() const = 0;

    const std::vector<int>& getColumnIndices() const
    {
        return column_indices_vector_;
    }

    const std::vector<ValueType>& getColumnTypes() const
    {
        return column_types_vector_;
    }

    int getColumnCount() const
    {
        return colCount_;
    }

    const std::string& getName() const
    {
        return name_;
    }

    const TupleSchema * getKeySchema() const
    {
        return m_keySchema;
    }

    virtual std::string debug() const;
    virtual std::string getTypeName() const = 0;

    virtual void ensureCapacity(uint32_t capacity) {}

    // print out info about lookup usage
    virtual void printReport();

    //TODO Useful implementation of == operator.
    virtual bool equals(const TableIndex *other) const;

    TableIndexScheme getScheme() const {
        return m_scheme;
    }

    virtual voltdb::IndexStats* getIndexStats();

protected:
    TableIndex(const TableIndexScheme &scheme);

    const TableIndexScheme m_scheme;
    TupleSchema* m_keySchema;
    std::string name_;
    std::vector<int> column_indices_vector_;
    std::vector<ValueType> column_types_vector_;
    ValueType* column_types_;
    int colCount_;
    bool is_unique_index_;
    int* column_indices_;

    // counters
    int m_lookups;
    int m_inserts;
    int m_deletes;
    int m_updates;
    TupleSchema *m_tupleSchema;

    // stats
    IndexStats m_stats;

private:
    ThreadLocalPool m_tlPool;
};

}

#endif
