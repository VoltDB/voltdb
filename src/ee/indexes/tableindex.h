/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

class AbstractExpression;

/**
 * Parameter for constructing TableIndex. TupleSchema, then key schema
 */
struct TableIndexScheme {
    TableIndexScheme() {
        tupleSchema = NULL;
    }

    TableIndexScheme(std::string a_name, TableIndexType a_type,
                     const std::vector<int32_t>& a_columnIndices,
                     const std::vector<AbstractExpression*>& a_indexedExpressions,
                     bool a_unique, bool a_countable,
                     const std::string& a_expressionsAsText,
                     const TupleSchema *a_tupleSchema) :
      name(a_name),
      type(a_type),
      columnIndices(a_columnIndices),
      indexedExpressions(a_indexedExpressions),
      unique(a_unique),
      countable(a_countable),
      expressionsAsText(a_expressionsAsText),
      tupleSchema(a_tupleSchema)
    {}

    // TODO: Remove this temporary backward-compatible test-only constructor -- this should go away soon, forcing
    // column index construction in the ee tests to provide rather than default the empty expressionsAsText string.
    // TODO: Better yet: move the call to construct the indexId:
    // Since the expressionsAsText is only used to additionally qualify the index id string, a process that is
    // initiated (rather awkwardly) from the TypeIndex constructor, it would be less trouble for the VoltDBEngine
    // (which already knows how) to always construct the entire indexId from the catalog index and pass THAT to
    // the TableIndexScheme constructor above (fully qualified by expressionsAsText) in place of expressionsAsText.
    // The small downside is the slightly larger string buffer being subject to the copying and recopying of
    // TableIndexScheme members on their way to their final embedding in the TableIndex.
    // This change would eliminate the mostly redundant method for TableIndexScheme-to-indexId conversion.
    // TableIndexScheme construction in most if not all ee tests could provide a dummy (empty or nonce) value
    // for indexId. Index Ids seem only to be of interest in catalog-driven processing.
    TableIndexScheme(std::string a_name, TableIndexType a_type,
                     const std::vector<int32_t>& a_columnIndices,
                     const std::vector<AbstractExpression*>& a_indexedExpressions,
                     bool a_unique, bool a_countable,
                     const TupleSchema *a_tupleSchema) :
      name(a_name),
      type(a_type),
      columnIndices(a_columnIndices),
      indexedExpressions(a_indexedExpressions),
      unique(a_unique),
      countable(a_countable),
      expressionsAsText(""),
      tupleSchema(a_tupleSchema)
    {
    }

    TableIndexScheme(const TableIndexScheme& other) :
      name(other.name),
      type(other.type),
      columnIndices(other.columnIndices),
      indexedExpressions(other.indexedExpressions),
      unique(other.unique),
      countable(other.countable),
      expressionsAsText(other.expressionsAsText),
      tupleSchema(other.tupleSchema)
    {}

    TableIndexScheme& operator=(const TableIndexScheme& other)
    {
        name = other.name;
        type = other.type;
        columnIndices = other.columnIndices;
        indexedExpressions = other.indexedExpressions;
        unique = other.unique;
        countable = other.countable;
        expressionsAsText = other.expressionsAsText;
        tupleSchema = other.tupleSchema;
        return *this;
    }

    static const std::vector<TableIndexScheme> noOptionalIndices()
    {
        return std::vector<TableIndexScheme>();
    }

    std::string name;
    TableIndexType type;
    std::vector<int32_t> columnIndices;
    std::vector<AbstractExpression*> indexedExpressions;
    bool unique;
    bool countable;
    std::string expressionsAsText;
    const TupleSchema *tupleSchema;
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
     * Update in place an index entry with a new tuple address
     */
    virtual bool replaceEntryNoKeyChange(const TableTuple &destinationTuple,
                                         const TableTuple &originalTuple) = 0;

    /**
     * Does the key out-of-line strings or binary data?
     * Used for an optimization when key values are the same.
     */
    virtual bool keyUsesNonInlinedMemory() = 0;

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

    /** retrieves from a primary key index the persistent tuple matching the given temp tuple */
    virtual TableTuple uniqueMatchingTuple(const TableTuple &searchTuple)
    {
        throwFatalException("Invoked TableIndex virtual method uniqueMatchingTuple which has no use on a non-unique index");
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
        return m_scheme.unique;
    }
    /**
     * Same as isUniqueIndex...
     */
    inline bool isCountableIndex() const
    {
        return m_scheme.countable;
    }

    virtual bool hasKey(const TableTuple *searchKey) = 0;

    /**
     * This function only supports countable tree index. It returns the counter value
     * equal or greater than the serarchKey. It will return the rank with the searchKey
     * in ascending order including itself.
     *
     * @parameter: isUpper means nothing to Unique index. For non-unique index, it will
     * return the high or low rank according to this boolean flag as true or false,respectively.
     *
     * @Return great than rank value as "m_entries.size() + 1"  for given
     * searchKey that is larger than all keys.
     */
    virtual int64_t getCounterGET(const TableTuple *searchKey, bool isUpper)
    {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterGET which has no implementation");
    }
    /**
     * This function only supports countable tree index. It returns the counter value
     * equal or less than the serarchKey. It will return the rank with the searchKey
     * in ascending order including itself.
     *
     * @parameter: isUpper means nothing to Unique index. For non-unique index, it will
     * return the high or low rank according to this boolean flag as true or false,respectively.
     *
     * @Return less than rank value as "m_entries.size()"  for given
     * searchKey that is larger than all keys.
     */
    virtual int64_t getCounterLET(const TableTuple *searchKey, bool isUpper)
    {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterLET which has no implementation");
    }


    virtual size_t getSize() const = 0;

    // Return the amount of memory we think is allocated for this
    // index.
    virtual int64_t getMemoryEstimate() const = 0;

    const std::vector<int>& getColumnIndices() const
    {
        return m_scheme.columnIndices;
    }

    // Provide an empty expressions vector to indicate a simple columns-only index.
    static const std::vector<AbstractExpression*>& simplyIndexColumns() {
        static std::vector<AbstractExpression*> emptyExpressionVector;
        return emptyExpressionVector;
    }

    const std::vector<AbstractExpression*>& getIndexedExpressions() const
    {
        return m_scheme.indexedExpressions;
    }


    const std::string& getName() const
    {
        return m_scheme.name;
    }

    void rename(std::string name) {
        if (m_scheme.name.compare(name) != 0) {
            m_scheme.name = name;
            IndexStats *stats = getIndexStats();
            if (stats) {
                stats->rename(name);
            }
        }
    }

    const std::string& getId() const
    {
        return m_id;
    }

    const TupleSchema *getKeySchema() const
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

    virtual voltdb::IndexStats* getIndexStats();

protected:
    const TupleSchema *getTupleSchema() const
    {
        return m_scheme.tupleSchema;
    }

    TableIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme);

    TableIndexScheme m_scheme;
    const TupleSchema * const m_keySchema;
    const std::string m_id;

    // counters
    int m_lookups;
    int m_inserts;
    int m_deletes;
    int m_updates;

    // stats
    IndexStats m_stats;

private:

    // This should always/only be required for unique key indexes used for primary keys.
    virtual TableIndex *cloneEmptyNonCountingTreeIndex() const {
        throwFatalException("Primary key index discovered to be non-unique or missing a cloneEmptyTreeIndex implementation.");
    }

    ThreadLocalPool m_tlPool;
};

}

#endif
