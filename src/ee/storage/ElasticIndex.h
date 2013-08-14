/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include <stx/btree.h>
#include <boost/iterator/iterator_facade.hpp>
#include "storage/TupleBlock.h"
#include "common/tabletuple.h"

namespace voltdb {

class ElasticIndexIterator;
class PersistentTable;

/// Hash value type.
typedef int64_t ElasticHash;

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
    char *m_ptr;
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
    iterator createIterator(ElasticHash lowerBound);

    /**
     * Get full const_iterator.
     */
    const_iterator createIterator() const;

    /**
     * Get partial const_iterator based on lower bound.
     */
    const_iterator createIterator(ElasticHash lowerBound) const;

  private:

    static ElasticHash generateHash(const PersistentTable &table, const TableTuple &tuple);

    static ElasticIndexKey generateKey(const PersistentTable &table, const TableTuple &tuple);
};

/**
 * Default constructor.
 */
inline ElasticIndexKey::ElasticIndexKey() :
    m_hash(0),
    m_ptr(NULL)
{}

/**
 * Full constructor.
 */
inline ElasticIndexKey::ElasticIndexKey(ElasticHash hash, char *ptr) :
    m_hash(hash),
    m_ptr(ptr)
{}

/**
 * Copy constructor.
 */
inline ElasticIndexKey::ElasticIndexKey(const ElasticIndexKey &other) :
    m_hash(other.m_hash),
    m_ptr(other.m_ptr)
{}

/**
 * Copy operator.
 */
inline const ElasticIndexKey &ElasticIndexKey::operator=(const ElasticIndexKey &other)
{
    m_hash = other.m_hash;
    m_ptr = other.m_ptr;
    return *this;
}

/**
 * Equality operator.
 */
inline bool ElasticIndexKey::operator==(const ElasticIndexKey &other) const
{
    return m_hash == other.m_hash && m_ptr == other.m_ptr;
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
    return m_ptr;
}

/**
 * Required less than comparison operator method for ElasticIndexKey.
 */
inline bool ElasticIndexComparator::operator()(
       const ElasticIndexKey &a, const ElasticIndexKey &b) const
{
    return (a.m_hash < b.m_hash || (a.m_hash == b.m_hash && a.m_ptr < b.m_ptr));
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
inline ElasticIndex::iterator ElasticIndex::createIterator(ElasticHash lowerBound)
{
    return lower_bound(ElasticIndexKey(lowerBound, NULL));
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
inline ElasticIndex::const_iterator ElasticIndex::createIterator(ElasticHash lowerBound) const
{
    return lower_bound(ElasticIndexKey(lowerBound, NULL));
}

/**
 * ElasticIndexKey streaming operator.
 */
inline std::ostream &operator<<(std::ostream &os, const ElasticIndexKey &key)
{
    os << std::hex << key.m_hash << ':' << reinterpret_cast<long>(key.m_ptr);
    return os;
}

} // namespace voltdb

#endif // ELASTIC_INDEX_H_
