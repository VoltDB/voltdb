/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef COMPACTINGHASHTABLE_H_
#define COMPACTINGHASHTABLE_H_

#include <cstdlib>
#include <utility>
#include <cassert>
#include <climits>
#include <iostream>
#include <cstring>
#include <sys/mman.h>
#include <boost/functional/hash.hpp>
#include "ContiguousAllocator.h"

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
    template<class K, class T, class H = boost::hash<K>, class EK = std::equal_to<K>, class ET = std::equal_to<T> >
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

#ifndef MEMCHECK

        // start with a roughly 512k hash table
        // (includes 64k 8B pointers)
        static const uint64_t BUCKET_INITIAL_COUNT = 1024 * 64;

        // 20000 HashNodes per chunk is about 625k / chunk
        static const uint64_t ALLOCATOR_CHUNK_SIZE = 20000;

#else // for MEMCHECK
        // for debugging with valgrind
        static const uint32_t BUCKET_INITIAL_COUNT = 8;
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

        struct HashNodeSmall {
            Key key;
            Data value;
            uint64_t hash;
            HashNode *nextInBucket;
        };

        HashNode **m_buckets;             // the array holding the buckets
        bool m_unique;                    // support unique
        uint32_t m_nodeSize;              // sizeof(HashNode) or sizeof(HashNodeSmall)
        uint64_t m_count;                 // number of items in the hash
        uint64_t m_uniqueCount;           // number of unique keys
        uint64_t m_size;                  // current bucket count
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
            iterator(const iterator &iter) : m_node(iter.m_node) {}

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
        bool insert(const Key &key, const Data &value);
        /** delete by key (unique only) */
        bool erase(const Key &key);
        /** delete by kv pair */
        bool erase(const Key &key, const Data &value);
        /** delete from iterator */
        bool erase(iterator &iter);
        /** STL-ish size() method */
        size_t size() const { return m_count; }

        /** Return bytes used for this index */
        size_t bytesAllocated() const { return m_allocator.bytesAllocated() + sizeof(m_buckets); }

        /** verification for debugging and testing */
        bool verify();

    protected:
        /** find, given a bucket/key */
        HashNode *find(const HashNode *bucket, const Key &key) const;
        /** find and exact match, given a bucket */
        HashNode *find(const HashNode *bucket, const Key &key, const Data &value) const;
        /** insert, given a bucket */
        bool insert(HashNode **bucket, uint64_t hash, const Key &key, const Data &value);
        /** remove, given a bucket and an exact node */
        bool remove(HashNode **bucket, HashNode *prevBucketNode, HashNode *keyHeadNode, HashNode *prevKeyNode, HashNode *node);
        bool removeUnique(HashNode **bucket, HashNode *prevBucketNode, HashNode *node);

        /** after remove, ensure memory for hashnodes is contiguous */
        void deleteAndFixup(HashNode *node);

        /** see if the hash needs to grow or shrink */
        void checkLoadFactor();
        /** grow/shrink the hash table */
        void resize(uint64_t newSize);
    };

    ///////////////////////////////////////////
    //
    // COMPACTING HASH TABLE CODE
    //
    ///////////////////////////////////////////

    template<class K, class T, class H, class EK, class ET>
    CompactingHashTable<K, T, H, EK, ET>::CompactingHashTable(bool unique, Hasher hasher, KeyEqChecker keyEq, DataEqChecker dataEq)
    : m_unique(unique),
    m_nodeSize(unique ? sizeof(HashNodeSmall) : sizeof(HashNode)),
    m_count(0),
    m_uniqueCount(0),
    m_size(BUCKET_INITIAL_COUNT),
    m_allocator(m_nodeSize, ALLOCATOR_CHUNK_SIZE),
    m_hasher(hasher),
    m_keyEq(keyEq),
    m_dataEq(dataEq)
    {
        // allocate the hash table and bzero it (bzero is crucial)
        void *memory = mmap(NULL, sizeof(HashNode*) * BUCKET_INITIAL_COUNT, PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        assert(memory);
        m_buckets = reinterpret_cast<HashNode**>(memory);
        memset(m_buckets, 0, sizeof(HashNode*) * BUCKET_INITIAL_COUNT);
    }

    template<class K, class T, class H, class EK, class ET>
    CompactingHashTable<K, T, H, EK, ET>::~CompactingHashTable() {
        // unlink all of the nodes, which will call destructors correctly
        for (size_t i = 0; i < m_size; ++i) {
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
        munmap(m_buckets, sizeof(HashNode*) * m_size);

        // when the allocator gets cleaned up, it will
        // free the memory used for nodes
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::iterator CompactingHashTable<K, T, H, EK, ET>::find(const Key &key) const {
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % m_size;
        const HashNode *foundNode = find(m_buckets[bucketOffset], key);
        return iterator(foundNode);
    }

    template<class K, class T, class H, class EK, class ET>
    typename CompactingHashTable<K, T, H, EK, ET>::iterator CompactingHashTable<K, T, H, EK, ET>::find(const Key &key, const Data &value) const {
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % m_size;
        const HashNode *foundNode = find(m_buckets[bucketOffset], key, value);
        return iterator(foundNode);
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::insert(const Key &key, const Data &value) {
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % m_size;
        return insert(&(m_buckets[bucketOffset]), hash, key, value);
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::erase(const Key &key) {
        assert(m_unique);
        HashNode *prevBucketNode = NULL;
        uint64_t hash = m_hasher(key);
        uint64_t bucketOffset = hash % m_size;

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
        uint64_t bucketOffset = hash % m_size;

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
    bool CompactingHashTable<K, T, H, EK, ET>::insert(HashNode **bucket, uint64_t hash, const Key &key, const Data &value) {
        HashNode *existing = find(*bucket, key);
        // protect unique constraint
        if (existing && m_unique) return false;

        // create a new node
        void *memory = m_allocator.alloc();
        assert(memory);
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
        return true;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET>::remove(HashNode **bucket, HashNode *prevBucketNode, HashNode *keyHeadNode, HashNode *prevKeyNode, HashNode *node) {
        assert(!m_unique);

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
        assert(m_unique);

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
        assert(node);

        // hash is empty now (after the recent delete)
        if (!m_count) {
            // safe to call the small destructor because the extra field
            //  for the larger HashNode isn't involved
            (reinterpret_cast<HashNodeSmall*>(node))->~HashNodeSmall();

            m_allocator.trim();
            assert(m_allocator.count() == m_count);
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
            assert(m_allocator.count() == m_count);
            return;
        }

        // find the bucket for the last node
        uint64_t bucketOffset = last->hash % m_size;

        // find the last node and what points to it
        HashNode *prevBucketNode = NULL, *keyHeadNode = NULL, *prevKeyNode = NULL;
        for (HashNode *n = m_buckets[bucketOffset]; n; n = n->nextInBucket) {
            prevKeyNode = NULL;
            keyHeadNode = n;
            if (m_unique) {
                if (n != last) continue; // not found

                // update things that point to the last node
                if (prevKeyNode)
                    prevKeyNode->nextWithKey = node;
                else
                    m_buckets[bucketOffset] = node;

                // copy the last node over the deleted node
                node->hash = last->hash;
                node->nextInBucket = last->nextInBucket;
                node->key = last->key;
                node->value = last->value;

                // destructor and memory release
                (reinterpret_cast<HashNodeSmall*>(node))->~HashNodeSmall();
                m_allocator.trim();
                assert(m_allocator.count() == m_count);

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
                    node->~HashNode();
                    m_allocator.trim();
                    assert(m_allocator.count() == m_count);

                    // done
                    return;
                }
            }
            prevBucketNode = n;
        }

        // not found
        assert(false);
    }

    template<class K, class T, class H, class EK, class ET>
    void CompactingHashTable<K, T, H, EK, ET>::checkLoadFactor() {
        uint64_t lf = (m_uniqueCount * 100) / m_size;
        uint64_t newSize = m_size;
        if (lf > MAX_LOAD_FACTOR)
            newSize <<= 1;
        else if(lf < MIN_LOAD_FACTOR)
            newSize >>= 1;
        if (newSize != m_size) {
            // make sure the hash doesn't over-shrink
            if (newSize >= BUCKET_INITIAL_COUNT)
                resize(newSize);
        }
    }

    template<class K, class T, class H, class EK, class ET>
    void CompactingHashTable<K, T, H, EK, ET>::resize(uint64_t newSize) {
        //std::cout << "SIZING BUFFER" << std::endl;
        //std::cout.flush();

        // create new double size buffer
        void *memory = mmap(NULL, sizeof(HashNode*) * newSize, PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        assert(memory);
        HashNode **newBuckets = reinterpret_cast<HashNode**>(memory);
        memset(newBuckets, 0, newSize * sizeof(HashNode*));

        // move all of the existing values
        for (uint32_t i = 0; i < m_size; ++i) {
            while (m_buckets[i]) {
                HashNode *node = m_buckets[i];
                m_buckets[i] = node->nextInBucket;

                uint64_t bucketOffset = node->hash % newSize;
                node->nextInBucket = newBuckets[bucketOffset];
                newBuckets[bucketOffset] = node;
            }
        }

        // swap the table buffers
        munmap(m_buckets, m_size * sizeof(HashNode*));
        m_buckets = newBuckets;
        m_size = newSize;
    }

    template<class K, class T, class H, class EK, class ET>
    bool CompactingHashTable<K, T, H, EK, ET> ::verify() {
        size_t manualCount = 0;

        for (uint64_t bucketi = 0; bucketi < m_size; ++bucketi) {
            for (HashNode *node = m_buckets[bucketi]; node; node = node->nextInBucket) {
                if (m_unique) {
                    uint64_t hash = m_hasher(node->key);
                    if (hash != node->hash) {
                        printf("Node hash doesn't match expected value.\n");
                        return false;
                    }
                    if ((hash % m_size) != bucketi) {
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
                        if ((hash % m_size) != bucketi) {
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

#endif // COMPACTINGHASHTABLE_H_
