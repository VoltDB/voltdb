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

#ifndef COMPACTINGMAP_H_
#define COMPACTINGMAP_H_

#include "ContiguousAllocator.h"

#include <cstdlib>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <limits>
#include <cassert>

typedef u_int32_t NodeCount;

#ifndef SUBCTMAX
#define SUBCTMAX INT32_MAX
#endif

#ifndef INVALIDCT
#define INVALIDCT 0
#endif

// MAXPOINTER will be reinterpret_cast back to UINTPTR_MAX
#ifndef MAXPOINTER
#define MAXPOINTER (reinterpret_cast<const void *>(UINTPTR_MAX))
#endif

namespace voltdb {

// The comparator the CompactingMap takes is a functor that returns -1 if a<b, 1
// if a>b, or 0 if a==b. This is different from the STL implementation where it
// takes a less functor. I'm providing a simple comparator here as the default.
template <typename T>
class comp {
private:
    std::less<T> m_less;

public:
    inline int operator()(const T &a, const T &b) const {
        if (m_less(a, b)) {
            return -1;
        } else if (m_less(b, a)) {
            return 1;
        } else {
            return 0;
        }
    }
};

template <typename T> inline void setPointerValue(T& t, const void * v) {}

// the template for KeyTypes don't contain a pointer to the tuple
template <typename Key, typename Data = const void*>
class NormalKeyValuePair : public std::pair<Key, Data> {
public:
    NormalKeyValuePair() {}
    NormalKeyValuePair(const Key &key, const Data &value) : std::pair<Key, Data>(key, value) {}

    const Key& getKey() const { return std::pair<Key, Data>::first; }
    const Data& getValue() const { return std::pair<Key, Data>::second; }
    void setKey(const Key &key) { std::pair<Key, Data>::first = key; }
    void setValue(const Data &value) { std::pair<Key, Data>::second = value; }

    // This function does nothing, and is only to offer the same API as PointerKeyValuePair.
    const void *setPointerValue(const void *value) { return NULL; }
};

/**
 * Basic Red-Black tree that is based on the pseudo-code from
 * Cormen's Algorithms book with a twist.
 *
 * The interface is somewhat stl::map-like, but is a loose subset with a few
 * simplifications and no STL inheritence compatibility.
 *
 * The ValueType is the same as the KeyValuePair type by default. CompactingSet
 * uses a different ValueType because it only cares about the key part in the
 * pair.
 *
 * The twist is that the memory storage for all nodes is tightly
 * packed into a buffer chain (CongtiguousAllocator). When nodes
 * are removed, other nodes are moved into the holes to keep
 * the memory contiguous. This prevents fragmenting of memory on
 * the heap (or in a pool) and allows deletion to return memory
 * to the operating system.
 *
 * Three issues to be aware of:
 * 1. Nodes can be moved in memory randomly. This currently calls
 *    copy constructors.
 * 2. Key types and Value types may not have their destructors
 *    called in all scenarios.
 * 3. Iterators are invalidated by any table mutation. Further,
 *    use of an iterator after a delete or insert may work fine
 *    or may crash. This could lead to a difficult to debug
 *    problem so please be aware of this issue.
 *
 * Some or all of these issues may be fixed in the future.
 *
 */

template<typename KeyValuePair, typename Compare, bool hasRank=false, typename ValueType=KeyValuePair>
class CompactingMap {
    typedef typename KeyValuePair::first_type Key;
    typedef typename KeyValuePair::second_type Data;

public:
    // The following required by the STL API.
    typedef Compare key_compare;
    typedef ValueType value_type;

protected:
    static const char RED = 0;
    static const char BLACK = 1;

    struct TreeNode {
        KeyValuePair kv;
        TreeNode *parent;
        TreeNode *left;
        TreeNode *right;
        char color;
        NodeCount subct;

        const Key &key() const { return kv.getKey(); };
        const Data &value() const { return kv.getValue(); };
        void setKey(const Key &value) { kv.setKey(value); }
        void setValue(const Data &value) { kv.setValue(value); }
    };

    int64_t m_count;
    TreeNode *m_root;
    ContiguousAllocator m_allocator;
    bool m_unique;

    // rather than NULL, most tree pointers that don't point
    // to nodes point to NIL. This is taken from Cormen and
    // makes some aspects easier to deal with.
    TreeNode NIL;

    // templated comparison function object
    // follows STL conventions
    Compare m_comper;

public:
    class iterator {
        friend class CompactingMap<KeyValuePair, Compare, hasRank, ValueType>;
    public:
        // The following are required by the STL API.
        typedef typename CompactingMap::value_type value_type;
        typedef value_type& reference;
        typedef value_type* pointer;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef ptrdiff_t difference_type;
    protected:
        const CompactingMap *m_map;
        TreeNode *m_node;
        iterator(const CompactingMap *m, TreeNode* x) : m_map(m), m_node(x) {}
        const KeyValuePair &pair() { return m_node->kv; }
    public:
        iterator() : m_map(NULL), m_node(NULL) {}
        iterator(const iterator &iter) : m_map(iter.m_map), m_node(iter.m_node) {}
        const Key &key() const { return m_node->key(); }
        const Data &value() const { return m_node->value(); }
        void setValue(const Data &value) { m_node->kv.setValue(value); }
        void moveNext() { m_node = m_map->successor(m_node); }
        void movePrev() { m_node = m_map->predecessor(m_node); }
        iterator& operator++() { moveNext(); return *this; }
        iterator operator++(int) { iterator tmp = *this; moveNext(); return tmp; }
        iterator& operator--() { movePrev(); return *this; }
        iterator operator--(int) { iterator tmp = *this; movePrev(); return tmp; }
        bool operator==(const iterator& x) const {
            return equals(x);
        }
        bool operator!=(const iterator& x) const {
            return !equals(x);
        }
        reference operator*() const { return getValueBasedOnType()(m_node->kv); }
        pointer operator->() const { return &getValueBasedOnType()(m_node->kv); }
        bool isEnd() const { return ((!m_map) || (m_node == &(m_map->NIL))); }
        bool equals(const iterator &iter) const {
            if (isEnd()) {
                return iter.isEnd();
            }
            return m_node == iter.m_node;
        }
    };

    class const_iterator {
        friend class CompactingMap<KeyValuePair, Compare, hasRank, ValueType>;
    public:
        // The following are required by the STL API.
        typedef typename CompactingMap::value_type value_type;
        typedef const value_type& reference;
        typedef const value_type* pointer;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef ptrdiff_t difference_type;
    protected:
        const CompactingMap *m_map;
        const TreeNode *m_node;
        const_iterator(const CompactingMap *m, const TreeNode* x) : m_map(m), m_node(x) {}
        const KeyValuePair &pair() const { return m_node->kv; }
    public:
        const_iterator() : m_map(NULL), m_node(NULL) {}
        const_iterator(const iterator &iter) : m_map(iter.m_map), m_node(iter.m_node) {}
        const_iterator(const const_iterator &iter) : m_map(iter.m_map), m_node(iter.m_node) {}
        const Key &key() const { return m_node->key(); }
        const Data &value() const { return m_node->value(); }
        void moveNext() { m_node = m_map->successor(m_node); }
        void movePrev() { m_node = m_map->predecessor(m_node); }
        const_iterator& operator++() { moveNext(); return *this; }
        const_iterator operator++(int) { const_iterator tmp = *this; moveNext(); return tmp; }
        const_iterator& operator--() { movePrev(); return *this; }
        const_iterator operator--(int) { const_iterator tmp = *this; movePrev(); return tmp; }
        bool operator==(const iterator& x) const {
            return equals(x);
        }
        bool operator==(const const_iterator& x) const {
            return equals(x);
        }
        bool operator!=(const iterator& x) const {
            return !equals(x);
        }
        bool operator!=(const const_iterator& x) const {
            return !equals(x);
        }
        reference operator*() const { return getValueBasedOnType()(m_node->kv); }
        pointer operator->() const { return &getValueBasedOnType()(m_node->kv); }
        bool isEnd() const { return ((!m_map) || (m_node == &(m_map->NIL))); }
        bool equals(const iterator &iter) const {
            if (isEnd()) {
                return iter.isEnd();
            }
            return m_node == iter.m_node;
        }
        bool equals(const const_iterator &iter) const {
            if (isEnd()) {
                return iter.isEnd();
            }
            return m_node == iter.m_node;
        }
    };

    // value_compare, key_comp() and value_comp() methods are required by STL.
    class value_compare {
        friend class CompactingMap<KeyValuePair, Compare, hasRank, ValueType>;
    protected:
        Compare m_keyCmp;

        inline value_compare(Compare comper) : m_keyCmp(comper) {}
    public:
        inline bool operator()(const value_type &a, const value_type &b) const {
            return m_keyCmp(a.first, b.first);
        }
    };
    inline key_compare key_comp() const { return m_comper; }
    inline value_compare value_comp() const { return value_compare(m_comper); }

    CompactingMap(bool unique = true, Compare comper = Compare());
    CompactingMap(const CompactingMap<KeyValuePair, Compare, hasRank, ValueType> &other);
    ~CompactingMap();

    // TODO: remove this. But two eecheck depend on this.
    bool insert(std::pair<Key, Data> value) { return (insert(value.first, value.second) == NULL); };
    // A syntactically convenient analog to CompactingHashTable's insert function
    const Data *insert(const Key &key, const Data &data);
    bool erase(const Key &key);
    bool erase(iterator &iter);
    void clear();

    iterator find(const Key &key) { return iterator(this, lookup(key)); }
    const_iterator find(const Key &key) const { return const_iterator(this, lookup(key)); }
    const_iterator findRank(int64_t ith) const { return const_iterator(this, lookupRank(ith)); }
    iterator findRank(int64_t ith) { return iterator(this, lookupRank(ith)); }
    size_t size() const { return m_count; }
    bool empty() const { return size() == 0; }
    iterator begin()
    {
        if (m_count == 0) {
            return iterator();
        }
        return iterator(this, minimum(m_root));
    }
    const_iterator begin() const
    {
        if (m_count == 0) {
            return const_iterator();
        }
        return const_iterator(this, minimum(m_root));
    }
    iterator end() { return iterator(this, &NIL); }
    const_iterator end() const { return const_iterator(this, &NIL); }
    iterator rbegin() {
        if (m_count == 0) {
            return iterator();
        }
        return iterator(this, maximum(m_root));
    }
    const_iterator rbegin() const {
        if (m_count == 0) {
            return const_iterator();
        }
        return const_iterator(this, maximum(m_root));
    }

    iterator lowerBound(const Key &key);
    const_iterator lowerBound(const Key &key) const;
    iterator upperBound(const Key &key);
    const_iterator upperBound(const Key &key) const;

    std::pair<iterator, iterator> equalRange(const Key &key);
    std::pair<const_iterator, const_iterator> equalRange(const Key &key) const;

    size_t bytesAllocated() const { return m_allocator.bytesAllocated(); }

    // TODO(xin): later rename it to rankLower
    // Must pass a key that already in map, or else return -1
    int64_t rankAsc(const Key& key) const;
    int64_t rankUpper(const Key& key) const;

    /**
     * For debugging: verify the RB-tree constraints are met. SLOW.
     */
    bool verify() const;
    bool verifyRank() const;

    inline bool operator==(const CompactingMap<KeyValuePair, Compare, hasRank, ValueType> &other) const
    {
        return (size() == other.size()) && std::equal(begin(), end(), other.begin());
    }

    inline bool operator!=(const CompactingMap<KeyValuePair, Compare, hasRank, ValueType> &other) const
    {
        return !(*this == other);
    }

protected:
    // main internal functions
    inline TreeNode *newNode()
    {
        void *memory = m_allocator.alloc();
        assert(memory);
        // placement new
        TreeNode *z = new(memory) TreeNode();
        z->left = z->right = z->parent = &NIL;
        if (hasRank) {
            z->subct = 1;
        }
        return z;
    }

    void copyRecursive(const TreeNode *fromNIL, const TreeNode *from, TreeNode *to);

    void erase(TreeNode *z);
    TreeNode *lookup(const Key &key) const;
    TreeNode *lookupRank(int64_t ith) const;

    TreeNode *findLowerBound(const Key &key) const;
    TreeNode *findUpperBound(const Key &key) const;

    inline int64_t getSubct(const TreeNode* x) const;
    inline void incSubct(TreeNode* x);
    inline void decSubct(TreeNode* x);
    inline void updateSubct(TreeNode* x);

    // static for iterator use
    TreeNode *minimum(const TreeNode *subRoot) const;
    TreeNode *maximum(const TreeNode *subRoot) const;
    TreeNode *successor(const TreeNode *x) const;
    TreeNode *predecessor(const TreeNode *x) const;

    // sub functions to make the magic happen
    void leftRotate(TreeNode *x);
    void rightRotate(TreeNode *x);
    void insertFixup(TreeNode *z);
    void deleteFixup(TreeNode *x);
    void fragmentFixup(TreeNode *x);

    // debugging and testing methods
    bool isReachableNode(const TreeNode* start, const TreeNode *dest) const;

    int verify(const TreeNode *n) const;
    int inOrderCounterChecking(const TreeNode *n) const;
    int fullCount(const TreeNode *n) const;

    inline int compareKeyRegardlessOfPointer(const Key& key, TreeNode *node) const;

    template<typename KVPair, typename V>
    struct getValue {
        inline V& operator()(KVPair &pair) const {
            return pair.first;
        }
        inline const V& operator()(const KVPair &pair) const {
            return pair.first;
        }
    };
    template<typename KVPair>
    struct getValue<KVPair, KVPair> {
        inline KVPair& operator()(KVPair &pair) const {
            return pair;
        }
        inline const KVPair& operator()(const KVPair &pair) const {
            return pair;
        }
    };
    typedef getValue<KeyValuePair, ValueType> getValueBasedOnType;
};

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::CompactingMap(bool unique, Compare comper)
    : m_count(0),
      m_root(&NIL),
      m_allocator(static_cast<int>(sizeof(TreeNode) - (hasRank ? 0 : sizeof(NodeCount))), static_cast<int>(10000)),
      m_unique(unique),
      m_comper(comper)
{
    NIL.left = NIL.right = NIL.parent = &NIL;
    NIL.color = BLACK;
    if (hasRank) {
        NIL.subct = INVALIDCT;
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::CompactingMap(
    const CompactingMap<KeyValuePair, Compare, hasRank, ValueType> &other)
    : m_count(other.m_count),
      m_root(&NIL),
      m_allocator(static_cast<int>(sizeof(TreeNode) - (hasRank ? 0 : sizeof(NodeCount))), static_cast<int>(10000)),
      m_unique(other.m_unique),
      m_comper(other.m_comper)
{
    NIL.left = NIL.right = NIL.parent = &NIL;
    NIL.color = BLACK;
    if (hasRank) {
        NIL.subct = INVALIDCT;
    }

    if (other.m_root != &other.NIL) {
        m_root = newNode();
        copyRecursive(&other.NIL, other.m_root, m_root);
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::copyRecursive(
    const TreeNode *fromNIL, const TreeNode *from, TreeNode *to)
{
    to->setKey(from->key());
    to->setValue(from->value());
    to->color = from->color;
    to->subct = from->subct;

    if (from->left != fromNIL) {
        to->left = newNode();
        to->left->parent = to;
        copyRecursive(fromNIL, from->left, to->left);
    }
    if (from->right != fromNIL) {
        to->right = newNode();
        to->right->parent = to;
        copyRecursive(fromNIL, from->right, to->right);
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::~CompactingMap()
{
    clear();
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::clear()
{
    iterator iter = begin();
    while (!iter.isEnd()) {
        iter.pair().~KeyValuePair();
        iter.moveNext();
    }

    m_root = &NIL;
    m_count = 0;
    m_allocator.clear();
    assert(m_allocator.count() == 0);
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
bool CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::erase(const Key &key)
{
    TreeNode *node = lookup(key);
    if (node == &NIL) {
        return false;
    }
    erase(node);
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
bool CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::erase(iterator &iter)
{
    assert(iter.m_node != &NIL);
    erase(iter.m_node);
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
const typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::Data *
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::insert(const Key &key, const Data &value)
{
    if (m_root != &NIL) {
        // find a place to put the new node
        TreeNode *y = &NIL;
        TreeNode *x = m_root;
        while (x != &NIL) {
            y = x;
            int cmp = m_comper(key, x->key());
            if (cmp < 0) {
                x = x->left;
            }
            else {
                // For non-unique indexes -- not really being used since unique tuple addresses
                // were added to keys -- new duplicates are (needlessly) forced to insert
                // after (to the right of) existing duplicates by falling through here.
                if (m_unique && (cmp == 0)) {
                    // Inserting exact matches fails for unique indexes.
                    // Undo the optimistic bumping of subcounts done already on the way down.
                    const Data *collidingData = &x->value();
                    if (hasRank) {
                        while (x != &NIL) {
                            x = x->parent;
                            decSubct(x);
                        }
                    }
                    return collidingData;
                }
                x = x->right;
            }

            if (hasRank) {
                incSubct(y);
            }
        }

        // create a new node
        TreeNode *z = newNode();
        z->setKey(key);
        z->setValue(value); // for PointerKeyType, this is a little duplicating process
        z->parent = y;
        z->color = RED;

        // stitch it in
        if (y == &NIL) {
            m_root = z;
        }
        else if (m_comper(z->key(), y->key()) < 0) {
            y->left = z;
        }
        else {
            y->right = z;
        }

        // rotate tree to balance if needed
        insertFixup(z);
    }
    else {
        // create a new node as root
        TreeNode *z = newNode();
        z->setKey(key);
        z->setValue(value); // for PointerKeyType, this is a little duplicating process
        z->color = BLACK;
        // make it root
        m_root = z;
    }
    m_count++;
    assert(m_allocator.count() == m_count);
    return NULL;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode *
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::findLowerBound(const Key &key) const
{
    TreeNode *x = m_root;
    TreeNode *y = const_cast<TreeNode*>(&NIL);
    while (x != &NIL) {
        int cmp = m_comper(x->key(), key);
        if (cmp < 0) {
            x = x->right;
        }
        else {
            y = x;
            x = x->left;
        }
    }
    return y;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::iterator
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::lowerBound(const Key &key)
{
    return iterator(this, findLowerBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::const_iterator
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::lowerBound(const Key &key) const
{
    return const_iterator(this, findLowerBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode *
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::findUpperBound(const Key &key) const
{
    Key tmpKey(key);
    setPointerValue(tmpKey, MAXPOINTER);
    TreeNode *x = m_root;
    TreeNode *y = const_cast<TreeNode*>(&NIL);
    while (x != &NIL) {
        int cmp = m_comper(x->key(), tmpKey);
        if (cmp <= 0) {
            x = x->right;
        }
        else {
            y = x;
            x = x->left;
        }
    }
    return y;

}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::iterator
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::upperBound(const Key &key)
{
    return iterator(this, findUpperBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::const_iterator
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::upperBound(const Key &key) const
{
    return const_iterator(this, findUpperBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename std::pair<typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::iterator,
                   typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::iterator>
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::equalRange(const Key &key)
{
    return std::pair<iterator, iterator>(lowerBound(key), upperBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename std::pair<typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::const_iterator,
                   typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::const_iterator>
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::equalRange(const Key &key) const
{
    return std::pair<const_iterator, const_iterator>(lowerBound(key), upperBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::erase(TreeNode *z)
{
    TreeNode *y, *x, *delnode = z;

    // find a replacement node to swap with
    if ((z->left == &NIL) || (z->right == &NIL)) {
        y = z;
    }
    else {
        y = successor(z);
    }

    if (y->left != &NIL) {
        x = y->left;
    }
    else {
        x = y->right;
    }

    x->parent = y->parent;

    if (y->parent == &NIL) {
        m_root = x;
    }
    else if (y == y->parent->left) {
        y->parent->left = x;
    }
    else {
        y->parent->right = x;
    }

    if (y != z) {
        z->kv = y->kv;
        delnode = y;
    }
    if (hasRank) {
        TreeNode *ct = delnode;
        while (ct != &NIL) {
            ct = ct->parent;
            decSubct(ct);
        }
    }

    if (y->color == BLACK) {
        deleteFixup(x);
    }
    m_count--;

    // move a node to fill this hole
    fragmentFixup(delnode);
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode *
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::lookup(const Key &key) const
{
    TreeNode *x = m_root;
    TreeNode *retval = const_cast<TreeNode*>(&NIL);
    while (x != &NIL) {
        int cmp = m_comper(x->key(), key);
        if (cmp < 0) {
            x = x->right;
        }
        else {
            if (cmp == 0) {
                //TODO: optimize for the (usual) unique index case, where
                // the first match found is known to be the only one:
                // if (m_unique) {
                //     return x;
                // }
                retval = x;
            }
            x = x->left;
        }
    }
    return retval;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::leftRotate(TreeNode *x)
{
    TreeNode *y = x->right;

    x->right = y->left;
    if (y->left != &NIL) {
        y->left->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == &NIL) {
        m_root = y;
    }
    else if (x == x->parent->left) {
        x->parent->left = y;
    }
    else {
        x->parent->right = y;
    }
    y->left = x;
    x->parent = y;

    if (hasRank) {
        updateSubct(x);
        updateSubct(y);
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::rightRotate(TreeNode *x)
{
    TreeNode *y = x->left;

    x->left = y->right;
    if (y->right != &NIL) {
        y->right->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == &NIL) {
        m_root = y;
    }
    else if (x == x->parent->right) {
        x->parent->right = y;
    }
    else {
        x->parent->left = y;
    }
    y->right = x;
    x->parent = y;

    if (hasRank) {
        updateSubct(x);
        updateSubct(y);
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::insertFixup(TreeNode *z)
{
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            TreeNode *y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            }
            else {
                if (z == z->parent->right) {
                    z = z->parent;
                    leftRotate(z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rightRotate(z->parent->parent);
            }
        }
        else {
            TreeNode *y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            }
            else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rightRotate(z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                leftRotate(z->parent->parent);
            }
        }
    }

    // added this myself
    m_root->color = BLACK;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::deleteFixup(TreeNode *x)
{
    while ((x != m_root) && (x->color == BLACK)) {
        if (x == x->parent->left) {
            TreeNode *w = x->parent->right;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                leftRotate(x->parent);
                w = x->parent->right;
            }

            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            }
            else {
                if (w->right->color == BLACK) {
                    w->left->color = BLACK;
                    w->color = RED;
                    rightRotate(w);
                    w = x->parent->right;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->right->color = BLACK;
                leftRotate(x->parent);
                x = m_root;
            }
        }
        else {
            TreeNode *w = x->parent->left;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                rightRotate(x->parent);
                w = x->parent->left;
            }

            if ((w->right->color == BLACK) && (w->left->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            }
            else {
                if (w->left->color == BLACK) {
                    w->right->color = BLACK;
                    w->color = RED;
                    leftRotate(w);
                    w = x->parent->left;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->left->color = BLACK;
                rightRotate(x->parent);
                x = m_root;
            }
        }
    }
    x->color = BLACK;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::fragmentFixup(TreeNode *X) {
    // tree is empty now (after the recent delete)
    if (m_count == 0) {
        X->~TreeNode();
        m_allocator.trim();
        assert(m_allocator.count() == m_count);
        return;
    }

    // last item allocated in our contiguous memory
    TreeNode *last = static_cast<TreeNode*>(m_allocator.last());

    // if deleting the last item
    if (last == X) {
        X->~TreeNode();
        m_allocator.trim();
        assert(m_allocator.count() == m_count);
        return;
    }

    // last should be a real node
    //assert(isReachableNode(m_root, last));

    // if there's a parent node, make it point to the hole
    if (last->parent != &NIL) {
        //assert(isReachableNode(m_root, last->parent));

        if (last->parent->left == last) {
            last->parent->left = X;
        }
        else {
            assert(last->parent->right == last);
            last->parent->right = X;
        }
    }

    // if there's children, make their parents point to hole
    if (last->left != &NIL) {
        last->left->parent = X;
    }
    if (last->right != &NIL) {
        last->right->parent = X;
    }

    // copy the last node over the deleted node
    assert(X != &NIL);
    X->parent = last->parent;
    X->left = last->left;
    X->right = last->right;
    X->color = last->color;
    X->kv = last->kv;
    if (hasRank) {
        X->subct = last->subct;
    }

    // fix the root pointer if needed
    if (last == m_root) {
        m_root = X;
    }

    last->~TreeNode();
    m_allocator.trim();
    assert(m_allocator.count() == m_count);
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::minimum(const TreeNode *subRoot) const
{
    while (subRoot->left != &NIL) {
        subRoot = subRoot->left;
    }
    return const_cast<TreeNode*>(subRoot);
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::maximum(const TreeNode *subRoot) const
{
    while (subRoot->right != &NIL) {
        subRoot = subRoot->right;
    }
    return const_cast<TreeNode*>(subRoot);
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::successor(const TreeNode *x) const
{
    if (x->right != &NIL) {
        return minimum(x->right);
    }
    TreeNode *y = x->parent;
    while ((y != &NIL) && (x == y->right)) {
        x = y;
        y = y->parent;
    }
    return y;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::predecessor(const TreeNode *x) const
{
    if (x == &NIL) {
        return maximum(m_root);
    }
    if (x->left != &NIL) {
        return maximum(x->left);
    }
    TreeNode *y = x->parent;
    while ((y != &NIL) && (x == y->left)) {
        x = y;
        y = y->parent;
    }
    return y;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
bool CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::isReachableNode(const TreeNode* start, const TreeNode *dest) const
{
    if (start == dest) {
        return true;
    }
    if ((start->left) && (isReachableNode(start->left, dest))) {
        return true;
    }
    if ((start->right) && (isReachableNode(start->right, dest))) {
        return true;
    }
    return false;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
inline int64_t CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::getSubct(const TreeNode* x) const
{
    if (x == &NIL) {
        return 0;
    }

    if (x->subct == INVALIDCT) {
        return getSubct(x->left) + getSubct(x->right) + 1;
    }
    // return 32_t, cast it to 64_t automatically
    return x->subct;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
inline void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::incSubct(TreeNode* x) {
    if (x == &NIL) {
        return;
    }
    if (x->subct == INVALIDCT) {
        return;
    }
    if (x->subct == SUBCTMAX) {
        x->subct = INVALIDCT;
    }
    else if (x->subct < SUBCTMAX) {
        x->subct++;
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
inline void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::decSubct(TreeNode* x)
{
    if (x == &NIL) {
        return;
    }
    if (x->subct == INVALIDCT) {
        updateSubct(x);
    } else {
        x->subct--;
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
inline void CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::updateSubct(TreeNode* x)
{
    if (x == &NIL) {
        return;
    }

    int64_t sumct = getSubct(x->left) + getSubct(x->right) + 1;
    if (sumct <= SUBCTMAX) {
        // assign the lower 32 value to subct
        x->subct = static_cast<NodeCount>(sumct);
    }
    else {
        x->subct = INVALIDCT;
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
int64_t CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::rankAsc(const Key& key) const
{
    if (!hasRank) {
        return -1;
    }
    TreeNode *n = lookup(key);
    // return -1 if the key passed in is not in the map
    if (n == &NIL) {
        return -1;
    }
    TreeNode *p = n;
    int64_t ct = 0,ctr = 0, ctl = 0;
    // only compare the "data" part of the key
    int m = compareKeyRegardlessOfPointer(key, m_root);
    if (m == 0) {
        if (m_root->right != &NIL) {
            ctr = getSubct(m_root->right);
        }
        ct = getSubct(m_root) - ctr;
        while(p->parent != &NIL) {
            if (compareKeyRegardlessOfPointer(key, p) == 0) {
                if (p->right != &NIL) {
                    if (compareKeyRegardlessOfPointer(key, p->right) == 0) {
                        ct-= getSubct(p->right);
                    }
                }
                ct--;
            }
            p = p->parent;
        }
    } else if (m > 0) {
        if (p->right != &NIL) {
            ctr = getSubct(p->right);
        }
        ct = getSubct(p) - ctr;
        while (p->parent != &NIL) {
            if (p->parent->right == p) {
                ct += getSubct(p->parent) - getSubct(p);
            }
            p = p->parent;
        }
    } else {
        if (p->left != &NIL) {
            ctl = getSubct(p->left);
        }
        ct = getSubct(p) - ctl - 1;
        while (p->parent != &NIL) {
            if (p->parent->left == p) {
                ct += getSubct(p->parent) - getSubct(p);
            }
            p = p->parent;
        }
        ct = getSubct(m_root) - ct;
    }
    return ct;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
int64_t CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::rankUpper(const Key& key) const
{
    if (!hasRank) {
        return -1;
    }
    if (m_unique) {
        return rankAsc(key);
    }
    TreeNode *n = lookup(key);
    // return -1 if the key passed in is not in the map
    if (n == &NIL) {
        return -1;
    }

    const_iterator it;
    it = upperBound(key);
    if (it.isEnd()) {
        return m_count;
    }
    return rankAsc(it.key()) - 1;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
typename CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::lookupRank(int64_t ith) const
{
    TreeNode *retval = const_cast<TreeNode*>(&NIL);
    if ((!hasRank) || m_root == &NIL || ith > getSubct(m_root)) {
        return retval;
    }

    TreeNode *x = m_root;
    int64_t rk = ith;
    int64_t xl = 0;
    while (x != &NIL && rk > 0) {
        if (x->left != &NIL) {
            xl = getSubct(x->left);
        }
        if (rk == xl + 1) {
            retval = x;
            rk = 0;
        } else if (rk < xl + 1) {
            x = x->left;
        } else {
            x = x->right;
            rk -= (xl + 1);
        }
        xl = 0;
    }
    return retval;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
bool CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::verifyRank() const
{
    if (!hasRank) {
        return true;
    }

    const_iterator it;
    int64_t rkasc;
    // iterate rank start from 1 to m_count
    for (int64_t i = 1; i <= m_count; i++) {
        it = findRank(i);
        if (lookup(it.key()) == &NIL) {
            printf("Can not find rank %ld node with key\n", (long)i);
            return false;
        }

        if (m_unique) {
            if ((rkasc = rankAsc(it.key())) != i) {
                printf("false: unique_rankAsc expected %ld, but got %ld\n", (long)i, (long)rkasc);
                return false;
            }
        } else {
            const Key k = it.key();
            // test rankUpper
            const_iterator up = upperBound(k);
            int64_t rkUpper;
            if (up.isEnd()) {
                if (m_count == i) {
                    rkUpper = rankUpper(k);
                    if (rkUpper != m_count) {
                        printf("false: multi_rankUpper expected %ld, but got %ld\n", (long)i, (long)rkUpper);
                        return false;
                    }
                }
            } else {
                up.movePrev();
                if (it.equals(up)) {
                    rkUpper = rankUpper(k);
                    if (rkUpper != i) {
                        printf("false: multi_rankUpper expected %ld, but got %ld\n", (long)i, (long)rkUpper);
                        return false;
                    }
                }
            }
            // test rankAsc
            rkasc = rankAsc(k);
            int64_t nc = 0;
            it.movePrev();
            while (k == it.key()) {
                nc++;
                it.movePrev();
            }
            if (rkasc + nc != i) {
                printf("false: multi_rankAsc %ld keys are the same", (long)nc);
                printf("false: multi_rankAsc expected %ld, but got %ld\n", (long)i, (long)rkasc);
                return false;
            }
        }
    }
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
bool CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::verify() const
{
    if (NIL.color != BLACK) {
        printf("NIL is red\n");
        return false;
    }
    if (NIL.left != &NIL) {
        printf("NIL left is not NIL\n");
        return false;
    }
    if (NIL.right != &NIL) {
        printf("NIL right is not NIL\n");
        return false;
    }
    if (!m_root) {
        return false;
    }
    if ((m_root == &NIL) && (m_count)) {
        return false;
    }
    if (m_root->color == RED) {
        return false;
    }
    if (m_root->parent != &NIL) {
        return false;
    }
    if (verify(m_root) < 0) {
        return false;
    }
    if (m_count != fullCount(m_root)) {
        return false;
    }

    // verify the sub tree nodes counter
    if (hasRank) {
        if (inOrderCounterChecking(m_root) < 0) {
            return false;
        }
    }
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
int CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::inOrderCounterChecking(const TreeNode *n) const
{
    int res = 0;
    if (n != &NIL) {
        if ((res = inOrderCounterChecking(n->left)) < 0) {
            return res;
        }
        // check counter for sub tree nodes
        int64_t ct = 1;
        if (n->left != &NIL) {
            ct += getSubct(n->left);
        }
        if (n->right != &NIL) {
            ct += getSubct(n->right);
        }
        if (ct != getSubct(n)) {
            printf("node counter is not correct, expected %ld but get %ld\n", (long)ct, (long)getSubct(n));
            return -1;
        }

        if ((res = inOrderCounterChecking(n->right)) < 0) {
            return res;
        }
    }
    return res;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
int CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::verify(const TreeNode *n) const
{
    // recursive stopping case
    if (n == NULL) {
        //TODO: This SHOULD return -1.
        return 0; // This should not happen. All leaves should terminate with &NIL.
    }
    if (n == &NIL)
        return 0;

    //printf("verify -> node %d\n", N->value);
    //fflush(stdout);

    // check children have a valid parent pointer
    if ((n->left != &NIL) && (n->left->parent != n)) {
        return -1;
    }
    if ((n->right != &NIL) && (n->right->parent != n)) {
        return -1;
    }

    // check for no two consecutive red nodes
    if (n->color == RED) {
        if ((n->left != &NIL) && (n->left->color == RED)) {
            return -1;
        }
        if ((n->right != &NIL) && (n->right->color == RED)) {
            return -1;
        }
    }

    // check for strict ordering
    if ((n->left != &NIL) && (m_comper(n->key(), n->left->key()) < 0)) {
        return -1;
    }
    if ((n->right != &NIL) && (m_comper(n->key(), n->right->key()) > 0)) {
        return -1;
    }

    // recursive step (compare black height)
    int leftBH = verify(n->left);
    int rightBH = verify(n->right);
    if (leftBH == -1) {
        return -1;
    }
    if (rightBH == -1) {
        return -1;
    }
    if (leftBH != rightBH) {
        return -1;
    }
    if (n->color == BLACK) {
        return leftBH + 1;
    }
    return leftBH;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
int CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::fullCount(const TreeNode *n) const
{
    if (n == &NIL) {
        return 0;
    }
    return fullCount(n->left) + fullCount(n->right) + 1;
}

template<typename KeyValuePair, typename Compare, bool hasRank, typename ValueType>
inline int
CompactingMap<KeyValuePair, Compare, hasRank, ValueType>::compareKeyRegardlessOfPointer(const Key& key, TreeNode *node) const
{
    // assume key's pointer field is NULL, if there is a pointer field in key
    const void *tmp = node->kv.setPointerValue(NULL);
    int rv = m_comper(key, node->key());
    node->kv.setPointerValue(tmp);
    return rv;
}

} // namespace voltdb

#endif // COMPACTINGMAP_H_
