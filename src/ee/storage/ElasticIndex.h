/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#ifndef ELASTIC_INDEX_H_
#define ELASTIC_INDEX_H_

#include <iostream>
#include <limits>
#include <stx/btree.h>
#include <boost/iterator/iterator_facade.hpp>
#include "storage/TupleBlock.h"
#include "common/tabletuple.h"

namespace voltdb {

class ElasticIndexIterator;
class PersistentTable;

/// Hash value type.
typedef int32_t ElasticHash;

/**
 * Data for the elastic index key.
 */
class ElasticIndexKey
{
    friend class ElasticIndexComparator;
    friend std::ostream &operator<<(std::ostream&, const ElasticIndexKey&);

  public:

    /**
     * Default constructor.
     */
    ElasticIndexKey();

    /**
     * Full constructor.
     */
    ElasticIndexKey(ElasticHash hash, char *ptr);

    /**
     * Full constructor that takes the casted pointer value directly.
     */
    ElasticIndexKey(ElasticHash hash, uintptr_t ptrVal);

    /**
     * Copy constructor.
     */
    ElasticIndexKey(const ElasticIndexKey &other);

    /**
     * Copy operator.
     */
    const ElasticIndexKey &operator=(const ElasticIndexKey &other);

    /**
     * Equality operator.
     */
    bool operator==(const ElasticIndexKey &other) const;

    /**
     * Hash accessor.
     */
    ElasticHash getHash() const;

    /**
     * Tuple address accessor.
     */
    char *getTupleAddress() const;

  private:

    ElasticHash m_hash;
    uintptr_t m_ptrVal;
};

/**
 * Required less than comparison operator for ElasticIndexKey.
 */
class ElasticIndexComparator
{
  public:
    bool operator()(const ElasticIndexKey &a, const ElasticIndexKey &b) const;
};

/**
 * The elastic index (set)
 */
class ElasticIndex : public stx::btree_set<ElasticIndexKey, ElasticIndexComparator,
                                    stx::btree_default_set_traits<ElasticIndexKey> >
{
    friend class ElasticIndexIterator;

  public:

    virtual ~ElasticIndex() {}

    /**
     * Return true if key is in the index (indirect from tuple).
     */
    bool has(const PersistentTable &table, const TableTuple &tuple) const;

    /**
     * Get the hash and tuple address if found.
     * Return true if key is in the index.
     */
    bool get(const PersistentTable &table, const TableTuple &tuple, ElasticIndexKey &key);

    /**
     * Add key to index (indirect from tuple).
     * Return true if it wasn't present and got added.
     */
    bool add(const PersistentTable &table, const TableTuple &tuple);

    /**
     * Add key to index (direct).
     * Return true if it wasn't present and got added.
     */
    bool add(const ElasticIndexKey &key);

    /**
     * Remove key from index.
     * Return true if the key was present and removed.
     */
    bool remove(const PersistentTable &table, const TableTuple &tuple);

    /**
     * Get full iterator.
     */
    iterator createIterator();

    /**
     * Get partial iterator based on lower bound.
     */
    iterator createLowerBoundIterator(ElasticHash lowerBound);

    /**
     * Get partial iterator based on upper bound.
     */
    iterator createUpperBoundIterator(ElasticHash upperBound);

    /**
     * Get full const_iterator.
     */
    const_iterator createIterator() const;

    /**
     * Get partial const_iterator based on lower bound.
     */
    const_iterator createLowerBoundIterator(ElasticHash lowerBound) const;

    /**
     * Get partial const_iterator based on upper bound.
     */
    const_iterator createUpperBoundIterator(ElasticHash upperBound) const;

    /**
     * Print the keys in the index
     */
    void printKeys(std::ostream &os, int32_t limit, const TupleSchema *schema, const PersistentTable &table) const;

  private:

    static ElasticHash generateHash(const PersistentTable &table, const TableTuple &tuple);

    static ElasticIndexKey generateKey(const PersistentTable &table, const TableTuple &tuple);
};

/**
 * Hash range for filtering.
 * The range specification is exclusive, specifically:
 *  from < to:
 *      from..to-1
 *  from >= to:
 *      from..max_int and min_int..to-1 (wraps around)
 * All possible value pairs are valid.
 */
class ElasticIndexHashRange
{
public:

    /**
     * Full constructor.
     */
    ElasticIndexHashRange(ElasticHash from, ElasticHash to);

    /**
     * Default constructor.
     */
    ElasticIndexHashRange();

    /**
     * Copy constructor.
     */
    ElasticIndexHashRange(const ElasticIndexHashRange &other);

    /**
     * From hash accessor.
     */
    ElasticHash getLowerBound() const;

    /**
     * From hash accessor.
     */
    ElasticHash getUpperBound() const;

private:

    ElasticHash m_from;
    ElasticHash m_to;
};

/**
 * Special purpose index tuple iterator that is bounded by a hash range.
 * Handles wrap-around when to <= from.
 */
class ElasticIndexTupleRangeIterator
{
public:

    /**
     * Constructor.
     */
    ElasticIndexTupleRangeIterator(ElasticIndex &index,
                                   const TupleSchema &schema,
                                   const ElasticIndexHashRange &range);

    /**
     * Move to next tuple, if available.
     * Update the tuple argument variable to access the current tuple.
     * Return false when no more are available.
     */
    bool next(TableTuple &tuple);

    /**
     * Reset iteration.
     */
    void reset();

    /**
     * access the range into which this iterator is built on
     */
    const ElasticIndexHashRange range() const {
        return m_range;
    }

private:

    ElasticIndex &m_index;
    const TupleSchema &m_schema;
    ElasticIndexHashRange m_range;
    ElasticIndex::iterator m_iter;
    ElasticIndex::iterator m_end;
};


/**
 * Default constructor.
 */
inline ElasticIndexKey::ElasticIndexKey() :
    m_hash(0),
    m_ptrVal(0)
{}

/**
 * Full constructor.
 */
inline ElasticIndexKey::ElasticIndexKey(ElasticHash hash, char *ptr) :
    m_hash(hash)
{
    // Cast pointer to unsigned integer so that it can be used in comparison
    // safely. Directly comparing less than of two pointers is undefined in C++.
    m_ptrVal = reinterpret_cast<uintptr_t>(ptr);
}

inline ElasticIndexKey::ElasticIndexKey(ElasticHash hash, uintptr_t ptrVal) :
    m_hash(hash),
    m_ptrVal(ptrVal)
{}

/**
 * Copy constructor.
 */
inline ElasticIndexKey::ElasticIndexKey(const ElasticIndexKey &other) :
    m_hash(other.m_hash),
    m_ptrVal(other.m_ptrVal)
{}

/**
 * Copy operator.
 */
inline const ElasticIndexKey &ElasticIndexKey::operator=(const ElasticIndexKey &other)
{
    m_hash = other.m_hash;
    m_ptrVal = other.m_ptrVal;
    return *this;
}

/**
 * Equality operator.
 */
inline bool ElasticIndexKey::operator==(const ElasticIndexKey &other) const
{
    return m_hash == other.m_hash && m_ptrVal == other.m_ptrVal;
}

/**
 * Hash accessor.
 */
inline ElasticHash ElasticIndexKey::getHash() const
{
    return m_hash;
}

/**
 * Tuple address accessor.
 */
inline char *ElasticIndexKey::getTupleAddress() const
{
    return reinterpret_cast<char *>(m_ptrVal);
}

/**
 * Required less than comparison operator method for ElasticIndexKey.
 */
inline bool ElasticIndexComparator::operator()(
       const ElasticIndexKey &a, const ElasticIndexKey &b) const
{
    return (a.m_hash < b.m_hash || (a.m_hash == b.m_hash && a.m_ptrVal < b.m_ptrVal));
}

/**
 * Internal method to generate a key from a table/tuple.
 */
inline ElasticIndexKey ElasticIndex::generateKey(const PersistentTable &table, const TableTuple &tuple)
{
    return ElasticIndexKey(generateHash(table, tuple), tuple.address());
}

/**
 * Return true if key is in the index (indirect from tuple).
 */
inline bool ElasticIndex::has(const PersistentTable &table, const TableTuple &tuple) const
{
    return exists(generateKey(table, tuple));
}

/**
 * Get the hash and tuple address if found.
 * Return true if key is in the index.
 */
inline bool ElasticIndex::get(const PersistentTable &table, const TableTuple &tuple,
                              ElasticIndexKey &key)
{
    ElasticIndexKey keyCmp = generateKey(table, tuple);
    if (exists(keyCmp)) {
        key = keyCmp;
        return true;
    }
    return false;
}

/**
 * Add key to index (indirect from tuple).
 * Return true if it wasn't present and needed to be added.
 */
inline bool ElasticIndex::add(const PersistentTable &table, const TableTuple &tuple)
{
    return add(generateKey(table, tuple));
}

/**
 * Add key to index (direct).
 * Return true if it wasn't present and needed to be added.
 */
inline bool ElasticIndex::add(const ElasticIndexKey &key)
{
    bool inserted = false;
    if (!exists(key)) {
        inserted = insert(key).second;
        assert(inserted);
    }
    return inserted;
}

/**
 * Remove key from index.
 * Return true if the key was present and removed.
 */
inline bool ElasticIndex::remove(const PersistentTable &table, const TableTuple &tuple)
{
    bool removed = false;
    ElasticIndexKey key = generateKey(table, tuple);
    if (exists(key)) {
        removed = this->erase(key);
    }
    return removed;
}

/**
 * Get full iterator.
 */
inline ElasticIndex::iterator ElasticIndex::createIterator()
{
    return begin();
}

/**
 * Get partial iterator based on lower bound.
 */
inline ElasticIndex::iterator ElasticIndex::createLowerBoundIterator(ElasticHash lowerBound)
{
    return lower_bound(ElasticIndexKey(lowerBound, (uintptr_t) 0));
}

/**
 * Get partial iterator based on upper bound.
 */
inline ElasticIndex::iterator ElasticIndex::createUpperBoundIterator(ElasticHash upperBound)
{
    return upper_bound(ElasticIndexKey(upperBound, std::numeric_limits<uintptr_t>::max()));
}

/**
 * Get full const_iterator.
 */
inline ElasticIndex::const_iterator ElasticIndex::createIterator() const
{
    return begin();
}

/**
 * Get partial const_iterator based on lower bound.
 */
inline ElasticIndex::const_iterator ElasticIndex::createLowerBoundIterator(ElasticHash lowerBound) const
{
    return lower_bound(ElasticIndexKey(lowerBound, (uintptr_t) 0));
}

/**
 * Get partial const_iterator based on upper bound.
 */
inline ElasticIndex::const_iterator ElasticIndex::createUpperBoundIterator(ElasticHash upperBound) const
{
    return upper_bound(ElasticIndexKey(upperBound, std::numeric_limits<uintptr_t>::max()));
}

/**
 * Print the keys in the index
 */
inline void ElasticIndex::printKeys(std::ostream &os, int32_t limit, const TupleSchema *schema, const PersistentTable &table) const {
    if (limit < 0) {
        limit = std::numeric_limits<int32_t>::max();
    }
    int32_t upto = 0;
    for (const_iterator itr = begin(); itr != end() && upto < limit; ++itr) {

        TableTuple tuple = TableTuple(itr->getTupleAddress(), schema);
        ElasticHash tupleHash = generateHash(table, tuple);

        os << *itr << ", is ";
        if (itr->getHash() != tupleHash) {
            os << "NOT ";
        }
        os << "a correct hash for its tuple address (pending delete: "
           << (tuple.isPendingDelete() ? "true)" : "false)")
           << std::endl;

        ++upto;
    }
}

/**
 * ElasticIndexKey streaming operator.
 */
inline std::ostream &operator<<(std::ostream &os, const ElasticIndexKey &key)
{
    os << key.m_hash << ':' << key.m_ptrVal;
    return os;
}

/**
 * Full constructor.
 */
inline ElasticIndexHashRange::ElasticIndexHashRange(ElasticHash from, ElasticHash to) :
    m_from(from), m_to(to)
{}

/**
 * Default constructor (full range).
 */
inline ElasticIndexHashRange::ElasticIndexHashRange() :
    // min->min covers all possible values, min->max would not.
    m_from(std::numeric_limits<int32_t>::min()), m_to(std::numeric_limits<int32_t>::max())
{}

/**
 * Copy constructor.
 */
inline ElasticIndexHashRange::ElasticIndexHashRange(const ElasticIndexHashRange &other) :
    m_from(other.m_from), m_to(other.m_to)
{}

/**
 * From hash accessor.
 */
inline ElasticHash ElasticIndexHashRange::getLowerBound() const
{
    return m_from;
}

/**
 * From hash accessor.
 */
inline ElasticHash ElasticIndexHashRange::getUpperBound() const
{
    return m_to;
}

} // namespace voltdb

#endif // ELASTIC_INDEX_H_
