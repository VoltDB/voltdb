/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#pragma once

#include "ContiguousAllocator.h"

#include <cstdlib>
#include <utility>
#include <common/debuglog.h>
#include <climits>
#include <iostream>
#include <cstring>
#include <sys/mman.h>
#include <boost/functional/hash.hpp>
#include <stdint.h>

namespace voltdb {

    /**
     * CompactingHashTable is similar in spirit to boost::unordered_map. It implements a map type built
     * on a chained-bucket hash-table that is templated on the key and value.
     *
     * It is special in that:
     * 1. It allocates all nodes in the bucket chains contiguously and will move things around in
     *    memory as nodes are removed to maintain this constraint. This should allow RSS to shrink
     *    as nodes are removed, regardless of memory layout.
     * 2. It is optimized slighltly for situations where hashing the key is expensive, such as
     *    in the case of complex multi-column VoltDB index keys containing out of line strings and such.
     * 3. It supports fewer operations than other STL-container-esqe data structures. For example, it
     *    doesn't support iteration over all values.
     * 4. It allocates over a megabyte when it only contains a single value. It's not as useful for
     *    smaller, more general usage.
     */
    template<class K, class T, class H = std::hash<K>, class EK = std::equal_to<K>, class ET = std::equal_to<T> >
    class CompactingHashTable {
    public:
        // typefefs just reduce the endless templating boilerplate
        typedef K Key;            // key type
        typedef T Data;           // value type
        typedef H Hasher;         // hash a value to a uint64_t
        typedef EK KeyEqChecker;  // compare two keys
        typedef ET DataEqChecker; // compare two values

        // grow when the hash table is 75% full
        // (new hash will be 37.5% full)
        static const uint64_t MAX_LOAD_FACTOR = 75; // %
        // shrink when the hash table is 15% full
        // (new hash will be 30% full)
        static const uint64_t MIN_LOAD_FACTOR = 15; // %

        static const uint64_t TABLE_SIZES[];

#ifndef MEMCHECK

        // start with a roughly 512k hash table
        // (includes 64k 8B pointers)
        static const uint64_t BUCKET_INITIAL_INDEX = 14;

        // 20000 HashNodes per chunk is about 625k / chunk
        static const uint64_t ALLOCATOR_CHUNK_SIZE = 20000;

#else // for MEMCHECK
        // for debugging with valgrind
        static const uint64_t BUCKET_INITIAL_INDEX = 0;
        static const uint64_t ALLOCATOR_CHUNK_SIZE = 2;

#endif // MEMCHECK

    protected:

        /**
         * The HashNode is pretty typical for a linked-list-as-bucket
         * implementation of a hash table. It does use up 8 bytes to
         * store the computed (pre modded) hash value of the data.
         * When hashing is expensive, this speeds up growing the
         * re-hashing process when doubling the table size.
         */
        struct HashNode {
            Key key;
            Data value;
            uint64_t hash;
            HashNode *nextInBucket;
            HashNode *nextWithKey;
        };

        /**
         * HashNodeSmall is some kind of dark evil magic. It is the same
         * as the HashNode, but missing the pointer that is only used
         * for multimap indices at the end. Using casts, unique indexes
         * will only use this type underneath, and thus save 8B per key.
         */
        struct HashNodeSmall {
            Key key;
            Data value;
            uint64_t hash;
            HashNode *nextInBucket;
        };

        HashNode **m_buckets;             // the array holding the buckets
        bool m_unique;                    // support unique
        uint64_t m_count;                 // number of items in the hash
        uint64_t m_uniqueCount;           // number of unique keys
        int m_sizeIndex;                  // current bucket count (from array)
        ContiguousAllocator m_allocator;  // allocator supporting compaction
        Hasher m_hasher;                  // instance of the hashing function
        KeyEqChecker m_keyEq;             // instance of the key eq checker
        DataEqChecker m_dataEq;           // instance of the value eq checker


    public:

        /**
         * Iterator class that will only iterate
         */
        class iterator {
            friend class CompactingHashTable;
        protected:
            // pointer to the actual value node
            HashNode *m_node;

            // protected constuctor just assigns values
            iterator(const HashNode *node) : m_node(const_cast<HashNode*>(node)) {}

        public:
            iterator() : m_node(NULL) {}

            Key &key() const { return m_node->key; }
            Data &value() const { return m_node->value; }
            void setValue(const Data &value) { m_node->value = value; }

            // move to the next hash node with the same key or make isEnd() true
            // (note: different than many other STL-ish implementations)
            void moveNext() { m_node = m_node->nextWithKey; }
            // equivalent to == containter.end() in STL-speak
            bool isEnd() const { return (!m_node); }
            // do two iterators point to the same node
            bool equals(iterator &iter) const { return m_node == iter.m_node; }
        };

        /** Constructor allows passing in instances for the hasher and eq checkers */
        CompactingHashTable(bool unique, Hasher hasher = Hasher(), KeyEqChecker keyEq = KeyEqChecker(), DataEqChecker dataEq = DataEqChecker());
        ~CompactingHashTable();

        /** simple find */
        iterator find(const Key &key) const;
        /** find an exact key/value match (optionaly searching by value first) */
        iterator find(const Key &key, const Data &value) const;
        /** simple insert */
        const Data *insert(const Key &key, const Data &value);
        /** delete by key (unique only) */
        bool erase(const Key &key);
        /** delete by kv pair */
        bool erase(const Key &key, const Data &value);
        /** delete from iterator */
        bool erase(iterator &iter);
        /** STL-ish size() method */
        size_t size() const { return m_count; }

        /** Return bytes used for this index */
        size_t bytesAllocated() const { return m_allocator.bytesAllocated() + TABLE_SIZES[m_sizeIndex] * sizeof(HashNode*); }

        /** verification for debugging and testing */
        bool verify();
        /** Do we have a cached last buffer?  This is used in testing. */
        bool hasCachedLastBuffer() const { return (m_allocator.hasCachedLastBuffer()); }

    protected:
        /** find, given a bucket/key */
        HashNode *find(const HashNode *bucket, const Key &key) const;
        /** find and exact match, given a bucket */
        HashNode *find(const HashNode *bucket, const Key &key, const Data &value) const;
        /** insert, given a bucket */
        const Data *insert(HashNode **bucket, uint64_t hash, const Key &key, const Data &value);
        /** remove, given a bucket and an exact node */
        bool remove(HashNode **bucket, HashNode *prevBucketNode, HashNode *keyHeadNode, HashNode *prevKeyNode, HashNode *node);
        bool removeUnique(HashNode **bucket, HashNode *prevBucketNode, HashNode *node);

        /** after remove, ensure memory for hashnodes is contiguous */
        void deleteAndFixup(HashNode *node);

        /** see if the hash needs to grow or shrink */
        void checkLoadFactor();
        /** grow/shrink the hash table */
        void resize(int newSizeIndex);
    };

    template<class K, class T, class H, class EK, class ET>
    const uint64_t CompactingHashTable<K, T, H, EK, ET>::TABLE_SIZES[] = {
        3,
        7,
        13,
        31,
        61,
        127,
        251,
        509,
        1021,
        2039,
        4093,
        8191,
        16381,
        32749,
        65521,
        131071,
        262139,
        524287,
        1048573,
        2097143,
        4194301,
        8388593,
        16777213,
        33554393,
        67108859,
        134217689,
        268435399,
        536870909,
        1073741789,
        2147483647,
        4294967291,
        8589934583
    };


    ///////////////////////////////////////////
    //
    // COMPACTING HASH TABLE CODE
    //
    ///////////////////////////////////////////

    template<class K, class T, class H, class EK, class ET>
    CompactingHashTable<K, T, H, EK, ET>::CompactingHashTable(bool unique, Hasher hasher, KeyEqChecker keyEq, DataEqChecker dataEq)
    : m_unique(unique),
    m_count(0),
    m_uniqueCount(0),
    m_sizeIndex(BUCKET_INITIAL_INDEX),
    m_allocator((int32_t)(unique ? sizeof(HashNodeSmall) : sizeof(HashNode)), ALLOCATOR_CHUNK_SIZE),
    m_hasher(hasher),
    m_keyEq(keyEq),
    m_dataEq(dataEq)
    {
        // allocate the hash table and bzero it (bzero is crucial)
        void *memory = mmap(NULL, sizeof(HashNode*) * TABLE_SIZES[m_sizeIndex], PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        vassert(memory);
        m_buckets = reinterpret_cast<HashNode**>(memory);
        memset(m_buckets, 0, sizeof(HashNode*) * TABLE_SIZES[m_sizeIndex]);
    }

    template<class K, class T, class H, class EK, class ET>
    CompactingHashTable<K, T, H, EK, ET>::~CompactingHashTable() {
        // unlink all of the nodes, which will call destructors correctly
        for (size_t i = 0; i < TABLE_SIZES[m_sizeIndex]; ++i) {
            while (m_buckets[i]) {
                HashNode *node = m_buckets[i];
                if (m_unique)
                    removeUnique(&(m_buckets[i]), NULL, node);
                else
                    remove(&(m_buckets[i]), NULL, node, NULL, node);
                // safe to call the small destructor because the extra field
                //  for the larger HashNode isn't involved
                (reinterpret_cast<HashNodeSmall*>(node))->~HashNodeSmall();
            }
        }

        // delete the hashtable
        munmap(m_buckets, sizeof(HashNode*) * TABLE_SIZES[m_sizeIndex]);

        // when the allocator gets cleaned up, it will
        // free the memory used for nodes
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::iterator CompactingHashTable<K, T, H, EK, ET>::find(const Key &key) const {
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % TABLE_SIZES[m_sizeIndex];
        const HashNode *foundNode = find(m_buckets[bucketOffset], key);
        return iterator(foundNode);
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::iterator CompactingHashTable<K, T, H, EK, ET>::find(const Key &key, const Data &value) const {
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % TABLE_SIZES[m_sizeIndex];
        const HashNode *foundNode = find(m_buckets[bucketOffset], key, value);
        return iterator(foundNode);
    }

    template<class K, class T, class H, class EK, class ET>
    const typename CompactingHashTable<K, T, H, EK, ET>::Data *CompactingHashTable<K, T, H, EK, ET>::insert(const Key &key, const Data &value) {
        uint64_t hash = m_hasher(key);

        uint64_t bucketOffset = hash % TABLE_SIZES[m_sizeIndex];
        return insert(&(m_buckets[bucketOffset]), hash, key, value);
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::erase(const Key &key) {
        vassert(m_unique);
        HashNode *prevBucketNode = NULL;
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % TABLE_SIZES[m_sizeIndex];

        for (HashNode *node = m_buckets[bucketOffset]; node; node = node->nextInBucket) {
            if (m_keyEq(node->key, key)) {
                removeUnique(&(m_buckets[bucketOffset]), prevBucketNode, node);
                deleteAndFixup(node);
                checkLoadFactor();
                return true;
            }
            prevBucketNode = node;
        }

        return false;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::erase(const Key &key, const Data &value) {
        HashNode *prevBucketNode = NULL, *keyHeadNode = NULL, *prevKeyNode = NULL;
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % TABLE_SIZES[m_sizeIndex];

        for (HashNode *node = m_buckets[bucketOffset]; node; node = node->nextInBucket) {
            if (m_keyEq(node->key, key)) {
                if (m_unique) {
                    if (!m_dataEq(node->value, value)) return false;
                    removeUnique(&(m_buckets[bucketOffset]), prevBucketNode, node);
                    deleteAndFixup(node);
                    checkLoadFactor();
                    return true;
                }
                keyHeadNode = node;
                for (node = keyHeadNode; node; node = node->nextWithKey) {
                    if (m_dataEq(node->value, value)) {
                        remove(&(m_buckets[bucketOffset]), prevBucketNode, keyHeadNode, prevKeyNode, node);
                        deleteAndFixup(node);
                        checkLoadFactor();
                        return true;
                    }
                    prevKeyNode = node;
                }
                break;
            }
            prevBucketNode = node;
        }

        return false;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::erase(iterator &iter) {
        if (m_unique) return erase(iter.key());
        else return erase(iter.key(), iter.value());
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::HashNode *CompactingHashTable<K, T, H, EK, ET>::find(const HashNode *bucket, const Key &key) const {
        for (HashNode *node = const_cast<HashNode*>(bucket); node; node = node->nextInBucket) {
            if (m_keyEq(node->key, key)) return node;
        }
        return NULL;
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::HashNode *CompactingHashTable<K, T, H, EK, ET>::find(const HashNode *bucket, const Key &key, const Data &value) const {
        for (HashNode *node = const_cast<HashNode*>(bucket); node; node = node->nextInBucket) {
            if (m_keyEq(node->key, key)) {
                for (HashNode *node2 = node; node2; node2 = node2->nextWithKey) {
                    if (m_dataEq(node2->value, value)) {
                        return node2;
                    }
                }
            }
        }
        return NULL;
    }

    template<class K, class T, class H, class EK, class ET>
    const typename CompactingHashTable<K, T, H, EK, ET>::Data *
    CompactingHashTable<K, T, H, EK, ET>::insert(HashNode **bucket, uint64_t hash, const Key &key, const Data &value) {
        HashNode *existing = find(*bucket, key);
        // protect unique constraint
        if (existing && m_unique) return &existing->value;

        // create a new node
        void *memory = m_allocator.alloc();
        vassert(memory);
        HashNode *newNode;
        // placement new
        if (m_unique) {
            newNode = reinterpret_cast<HashNode*>(new(memory) HashNodeSmall());
        }
        else {
            newNode = new(memory) HashNode();
            newNode->nextWithKey = NULL;
        }

        newNode->hash = hash;
        newNode->key = key;
        newNode->value = value;
        m_count++;

        if (existing) {
            // note if here, using non-unique path
            newNode->nextWithKey = existing->nextWithKey;
            existing->nextWithKey = newNode;
            newNode->nextInBucket = NULL;
        }
        else {
            newNode->nextInBucket = *bucket;
            *bucket = newNode;
            m_uniqueCount++;
        }

        checkLoadFactor();
        return NULL;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::remove(HashNode **bucket, HashNode *prevBucketNode, HashNode *keyHeadNode, HashNode *prevKeyNode, HashNode *node) {
        vassert(!m_unique);

        // if not in the main list from the bucket
        // but rather linked off of an original key
        if (keyHeadNode != node) {
            prevKeyNode->nextWithKey = node->nextWithKey;
            m_count--;
            return true;
        }

        // if nothing is linked from this key
        if (node->nextWithKey == NULL) {
            if (*bucket == node)
                *bucket = node->nextInBucket;
            else {
                // remove a node in the chain
                prevBucketNode->nextInBucket = node->nextInBucket;
            }
            m_uniqueCount--;
            m_count--;
            return true;
        }

        // if this is the head of a set of unique values
        if (*bucket == node)
            *bucket = node->nextWithKey;
        else {
            // remove a node in the chain
            prevBucketNode->nextInBucket = node->nextWithKey;
        }
        node->nextWithKey->nextInBucket = node->nextInBucket;

        m_count--;
        return true;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::removeUnique(HashNode **bucket, HashNode *prevBucketNode, HashNode *node) {
        vassert(m_unique);

        if (*bucket == node)
            *bucket = node->nextInBucket;
        else {
            // remove a node in the chain
            prevBucketNode->nextInBucket = node->nextInBucket;
        }
        m_uniqueCount--;
        m_count--;
        return true;
    }

    template<class K, class T, class H, class EK, class ET>
    void CompactingHashTable<K, T, H, EK, ET>::deleteAndFixup(HashNode *node) {
        vassert(node);

        // hash is empty now (after the recent delete)
        if (!m_count) {
            // safe to call the small destructor because the extra field
            //  for the larger HashNode isn't involved
            (reinterpret_cast<HashNodeSmall*>(node))->~HashNodeSmall();

            m_allocator.trim();
            vassert(m_allocator.count() == m_count);
            return;
        }

        // last item allocated in our contiguous memory
        HashNode *last = static_cast<HashNode*>(m_allocator.last());

        // if deleting the last item
        if (last == node) {
            // safe to call the small destructor because the extra field
            //  for the larger HashNode isn't involved
            (reinterpret_cast<HashNodeSmall*>(node))->~HashNodeSmall();

            m_allocator.trim();
            vassert(m_allocator.count() == m_count);
            return;
        }

        // find the bucket for the last node
        uint64_t bucketOffset = last->hash % TABLE_SIZES[m_sizeIndex];

        // find the last node and what points to it
        HashNode *prevBucketNode = NULL, *keyHeadNode = NULL, *prevKeyNode = NULL;
        for (HashNode *n = m_buckets[bucketOffset]; n; n = n->nextInBucket) {
            prevKeyNode = NULL;
            keyHeadNode = n;
            if (m_unique) {
                if (n != last) {
                    prevBucketNode = n;
                    continue; // not found
                }

                // update things that point to the last node
                if (prevBucketNode) {
                    prevBucketNode->nextInBucket = node;
                }
                else {
                    m_buckets[bucketOffset] = node;
                }

                // copy the last node over the deleted node
                node->hash = last->hash;
                node->nextInBucket = last->nextInBucket;
                node->key = last->key;
                node->value = last->value;

                // destructor and memory release
                (reinterpret_cast<HashNodeSmall*>(last))->~HashNodeSmall();
                m_allocator.trim();
                vassert(m_allocator.count() == m_count);

                // done
                return;

            }
            else {
                for (HashNode *n2 = keyHeadNode; n2; n2 = n2->nextWithKey) {
                    if (n2 != last) {
                        prevKeyNode = n2;
                        continue; // not found
                    }

                    // update things that point to the last node
                    if (prevKeyNode) {
                        prevKeyNode->nextWithKey = node;
                    }
                    else {
                        if (prevBucketNode) {
                            prevBucketNode->nextInBucket = node;
                        }
                        else {
                            m_buckets[bucketOffset] = node;
                        }
                    }

                    // copy the last node over the deleted node
                    node->hash = last->hash;
                    node->nextInBucket = last->nextInBucket;
                    node->nextWithKey = last->nextWithKey;
                    node->key = last->key;
                    node->value = last->value;

                    // destructor and memory release
                    last->~HashNode();
                    m_allocator.trim();
                    vassert(m_allocator.count() == m_count);

                    // done
                    return;
                }
            }
            prevBucketNode = n;
        }

        // not found
        vassert(false);
    }

    template<class K, class T, class H, class EK, class ET>
    void CompactingHashTable<K, T, H, EK, ET>::checkLoadFactor() {
        uint64_t lf = (m_uniqueCount * 100) / TABLE_SIZES[m_sizeIndex];
        int newSizeIndex = m_sizeIndex;
        if (lf > MAX_LOAD_FACTOR) {
            newSizeIndex++;
        }
        else if(lf < MIN_LOAD_FACTOR) {
            // make sure the hash doesn't over-shrink
            if (newSizeIndex != BUCKET_INITIAL_INDEX) {
                newSizeIndex--;
            }
        }
        if (newSizeIndex != m_sizeIndex) {
            resize(newSizeIndex);
        }
    }

    template<class K, class T, class H, class EK, class ET>
    void CompactingHashTable<K, T, H, EK, ET>::resize(int newSizeIndex) {
        //std::cout << "DEBUG SIZING BUFFER" << newSizeIndex << std::endl;
        //std::cout.flush();

        // create new double size buffer
        void *memory = mmap(NULL, sizeof(HashNode*) * TABLE_SIZES[newSizeIndex], PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        vassert(memory);
        HashNode **newBuckets = reinterpret_cast<HashNode**>(memory);
        memset(newBuckets, 0, TABLE_SIZES[newSizeIndex] * sizeof(HashNode*));

        // move all of the existing values
        for (uint32_t i = 0; i < TABLE_SIZES[m_sizeIndex]; ++i) {
            while (m_buckets[i]) {
                HashNode *node = m_buckets[i];
                m_buckets[i] = node->nextInBucket;

                uint64_t bucketOffset = node->hash % TABLE_SIZES[newSizeIndex];
                node->nextInBucket = newBuckets[bucketOffset];
                newBuckets[bucketOffset] = node;
            }
        }

        // swap the table buffers
        munmap(m_buckets, TABLE_SIZES[m_sizeIndex] * sizeof(HashNode*));
        m_buckets = newBuckets;
        m_sizeIndex = newSizeIndex;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET> ::verify() {
        size_t manualCount = 0;

        for (uint64_t bucketi = 0; bucketi < TABLE_SIZES[m_sizeIndex]; ++bucketi) {
            for (HashNode *node = m_buckets[bucketi]; node; node = node->nextInBucket) {
                if (m_unique) {
                    uint64_t hash = m_hasher(node->key);
                    if (hash != node->hash) {
                        printf("Node hash doesn't match expected value.\n");
                        return false;
                    }
                    if ((hash % TABLE_SIZES[m_sizeIndex]) != bucketi) {
                        printf("Node hash doesn't match expected bucket index.\n");
                        return false;
                    }

                    ++manualCount;
                }
                else {
                    for (HashNode *node2 = node; node2; node2 = node2->nextWithKey) {
                        uint64_t hash = m_hasher(node2->key);
                        if (hash != node2->hash) {
                            printf("Node hash doesn't match expected value.\n");
                            return false;
                        }
                        if ((hash % TABLE_SIZES[m_sizeIndex]) != bucketi) {
                            printf("Node hash doesn't match expected bucket index.\n");
                            return false;
                        }

                        ++manualCount;
                    }
                }
            }
        }

        if (manualCount != m_count) {
            printf("Found %d nodes by walking all buffers, but expected %d nodes.\n",
                   (int) manualCount, (int) m_count);
            return false;
        }
        return true;
    }
}

