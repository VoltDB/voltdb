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
#include "storage/persistenttable.h"
#include "storage/TupleBlock.h"
#include "common/tabletuple.h"

namespace voltdb {

class ElasticIndexIterator;

/**
 * Elastic hash value container with some useful/convenient operations.
 */
class ElasticHash
{
    friend std::ostream &operator<<(std::ostream&, const ElasticHash&);

  public:

    /**
     * Default constructor
     */
    ElasticHash();

    /**
     * Full constructor
     */
    ElasticHash(const PersistentTable &table, const TableTuple &tuple);

   /**
     * Copy constructor
     */
    ElasticHash(const ElasticHash &other);

    /**
     * Assignment operator
     */
    const ElasticHash &operator=(const ElasticHash &other);

    /**
     * Less than operator
     */
    bool operator<(const ElasticHash &other) const;

    /**
     * Equality operator
     */
    bool operator==(const ElasticHash &other) const;

  private:

    // Elastic hashes are 16 bytes, but we only keep the least significant 8 bytes.
    int64_t m_hashValue;
};

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
    ElasticIndexKey(const PersistentTable &table, const TableTuple &tuple);

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
     * Return true if key is in the index.
     */
    bool has(const PersistentTable &table, const TableTuple &tuple);

    /**
     * Get the hash and tuple address if found.
     * Return true if key is in the index.
     */
    bool get(const PersistentTable &table, const TableTuple &tuple, ElasticIndexKey &key);

    /**
     * Add key to index.
     * Return true if it wasn't present and got added.
     */
    bool add(const PersistentTable &table, const TableTuple &tuple);

    /**
     * Remove key from index.
     * Return true if the key was present and removed.
     */
    bool remove(const PersistentTable &table, const TableTuple &tuple);
};

/**
 * Default constructor
 */
inline ElasticHash::ElasticHash() :
    m_hashValue(0)
{}

/**
 * Full constructor
 */
inline ElasticHash::ElasticHash(const PersistentTable &table, const TableTuple &tuple)
{
    int64_t hashValues[2];
    tuple.getNValue(table.partitionColumn()).murmurHash3(hashValues);
    // Only the least significant 8 bytes is used.
    m_hashValue = hashValues[0];
}

/**
 * Copy constructor
 */
inline ElasticHash::ElasticHash(const ElasticHash &other) :
    m_hashValue(other.m_hashValue)
{}

/**
 * Assignment operator
 */
inline const ElasticHash &ElasticHash::operator=(const ElasticHash &other)
{
    m_hashValue = other.m_hashValue;
    return *this;
}

/**
 * Less than operator
 */
inline bool ElasticHash::operator<(const ElasticHash &other) const
{
    return (m_hashValue < other.m_hashValue);
}

/**
 * Equality operator
 */
inline bool ElasticHash::operator==(const ElasticHash &other) const
{
    return (m_hashValue == other.m_hashValue);
}

/**
 * ElasticHash streaming operator.
 */
inline std::ostream &operator<<(std::ostream &os, const ElasticHash &hash)
{
    os << std::setfill('0') << std::setw(16) << std::hex << hash.m_hashValue;
    return os;
}

/**
 * Default constructor.
 */
inline ElasticIndexKey::ElasticIndexKey() :
    m_ptr(NULL)
{}

/**
 * Full constructor.
 */
inline ElasticIndexKey::ElasticIndexKey(const PersistentTable &table, const TableTuple &tuple) :
    m_hash(table, tuple),
    m_ptr(tuple.address())
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
 * Return true if key is in the index.
 */
inline bool ElasticIndex::has(const PersistentTable &table, const TableTuple &tuple)
{
    return exists(ElasticIndexKey(table, tuple));
}

/**
 * Get the hash and tuple address if found.
 * Return true if key is in the index.
 */
inline bool ElasticIndex::get(const PersistentTable &table, const TableTuple &tuple,
                              ElasticIndexKey &key)
{
    ElasticIndexKey keyCmp(table, tuple);
    if (exists(keyCmp)) {
        key = keyCmp;
        return true;
    }
    return false;
}

/**
 * Add key to index.
 * Return true if it wasn't present and needed to be added.
 */
inline bool ElasticIndex::add(const PersistentTable &table, const TableTuple &tuple)
{
    bool inserted = false;
    ElasticIndexKey key(table, tuple);
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
    ElasticIndexKey key(table, tuple);
    if (exists(key)) {
        removed = this->erase(key);
    }
    return removed;
}

/**
 * ElasticIndexKey streaming operator.
 */
inline std::ostream &operator<<(std::ostream &os, const ElasticIndexKey &key)
{
    os << key.m_hash << ':' << std::hex << reinterpret_cast<long>(key.m_ptr);
    return os;
}

} // namespace voltdb

#endif // ELASTIC_INDEX_H_
