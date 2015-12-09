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
 * The interface is stl::map-like, but is a loose subset with a few
 * simplifications and no STL inheritence compatibility.
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
 * 4. Iterators have no overloaded operators yet. You can't
 *    compare them using ==. Compare keys and values instead.
 *
 * Some or all of these issues may be fixed in the future.
 *
 */

template<typename KeyValuePair, typename Compare, bool hasRank=false>
class CompactingMap {
    typedef typename KeyValuePair::first_type Key;
    typedef typename KeyValuePair::second_type Data;
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
        friend class CompactingMap<KeyValuePair, Compare, hasRank>;
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
        bool isEnd() const { return ((!m_map) || (m_node == &(m_map->NIL))); }
        bool equals(const iterator &iter) const {
            if (isEnd()) {
                return iter.isEnd();
            }
            return m_node == iter.m_node;
        }
    };

    CompactingMap(bool unique, Compare comper);
    ~CompactingMap();

    // TODO: remove this. But two eecheck depend on this.
    bool insert(std::pair<Key, Data> value) { return insert(value.first, value.second); };
    // A syntactically convenient analog to CompactingHashTable's insert function
    bool insert(const Key &key, const Data &data);
    bool erase(const Key &key);
    bool erase(iterator &iter);

    iterator find(const Key &key) const { return iterator(this, lookup(key)); }
    iterator findRank(int64_t ith) const { return iterator(this, lookupRank(ith)); }
    int64_t size() const { return m_count; }
    iterator begin() const
    {
        if (m_count == 0) {
            return iterator();
        }
        return iterator(this, minimum(m_root));
    }
    iterator rbegin() const {
        if (m_count == 0) {
            return iterator();
        }
        return iterator(this, maximum(m_root));
    }

    iterator lowerBound(const Key &key) const;
    iterator upperBound(const Key &key) const;

    std::pair<iterator, iterator> equalRange(const Key &key) const;

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

protected:
    // main internal functions
    void erase(TreeNode *z);
    TreeNode *lookup(const Key &key) const;
    TreeNode *lookupRank(int64_t ith) const;

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
};

template<typename KeyValuePair, typename Compare, bool hasRank>
CompactingMap<KeyValuePair, Compare, hasRank>::CompactingMap(bool unique, Compare comper)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
CompactingMap<KeyValuePair, Compare, hasRank>::~CompactingMap()
{
    iterator iter = begin();
    while (!iter.isEnd()) {
        iter.pair().~KeyValuePair();
        iter.moveNext();
    }
}

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::erase(const Key &key)
{
    TreeNode *node = lookup(key);
    if (node == &NIL) {
        return false;
    }
    erase(node);
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::erase(iterator &iter)
{
    assert(iter.m_node != &NIL);
    erase(iter.m_node);
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::insert(const Key &key, const Data &value)
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
                    if (hasRank) {
                        while (x != &NIL) {
                            x = x->parent;
                            decSubct(x);
                        }
                    }
                    return false;
                }
                x = x->right;
            }

            if (hasRank) {
                incSubct(y);
            }
        }

        // create a new node
        void *memory = m_allocator.alloc();
        assert(memory);
        // placement new
        TreeNode *z = new(memory) TreeNode();
        z->setKey(key);
        z->setValue(value); // for PointerKeyType, this is a little duplicating process
        z->left = z->right = &NIL;
        z->parent = y;
        z->color = RED;
        if (hasRank) {
            z->subct = 1;
        }

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
        void *memory = m_allocator.alloc();
        assert(memory);
        // placement new
        TreeNode *z = new(memory) TreeNode();
        z->setKey(key);
        z->setValue(value); // for PointerKeyType, this is a little duplicating process
        z->left = z->right = &NIL;
        z->parent = &NIL;
        z->color = BLACK;
        if (hasRank) {
            z->subct = 1;
        }
        // make it root
        m_root = z;
    }
    m_count++;
    assert(m_allocator.count() == m_count);
    return true;
}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::iterator
CompactingMap<KeyValuePair, Compare, hasRank>::lowerBound(const Key &key) const
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
    return iterator(this, y);
}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::iterator
CompactingMap<KeyValuePair, Compare, hasRank>::upperBound(const Key &key) const
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
    return iterator(this, y);

}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename std::pair<typename CompactingMap<KeyValuePair, Compare, hasRank>::iterator,
                   typename CompactingMap<KeyValuePair, Compare, hasRank>::iterator>
CompactingMap<KeyValuePair, Compare, hasRank>::equalRange(const Key &key) const
{
    return std::pair<iterator, iterator>(lowerBound(key), upperBound(key));
}

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::erase(TreeNode *z)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode *
CompactingMap<KeyValuePair, Compare, hasRank>::lookup(const Key &key) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::leftRotate(TreeNode *x)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::rightRotate(TreeNode *x)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::insertFixup(TreeNode *z)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::deleteFixup(TreeNode *x)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
void CompactingMap<KeyValuePair, Compare, hasRank>::fragmentFixup(TreeNode *X) {
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

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank>::minimum(const TreeNode *subRoot) const
{
    while (subRoot->left != &NIL) {
        subRoot = subRoot->left;
    }
    return const_cast<TreeNode*>(subRoot);
}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank>::maximum(const TreeNode *subRoot) const
{
    while (subRoot->right != &NIL) {
        subRoot = subRoot->right;
    }
    return const_cast<TreeNode*>(subRoot);
}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank>::successor(const TreeNode *x) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank>::predecessor(const TreeNode *x) const
{
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

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::isReachableNode(const TreeNode* start, const TreeNode *dest) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
inline int64_t CompactingMap<KeyValuePair, Compare, hasRank>::getSubct(const TreeNode* x) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
inline void CompactingMap<KeyValuePair, Compare, hasRank>::incSubct(TreeNode* x) {
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

template<typename KeyValuePair, typename Compare, bool hasRank>
inline void CompactingMap<KeyValuePair, Compare, hasRank>::decSubct(TreeNode* x)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
inline void CompactingMap<KeyValuePair, Compare, hasRank>::updateSubct(TreeNode* x)
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

template<typename KeyValuePair, typename Compare, bool hasRank>
int64_t CompactingMap<KeyValuePair, Compare, hasRank>::rankAsc(const Key& key) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
int64_t CompactingMap<KeyValuePair, Compare, hasRank>::rankUpper(const Key& key) const
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

    iterator it;
    it = upperBound(key);
    if (it.isEnd()) {
        return m_count;
    }
    return rankAsc(it.key()) - 1;
}

template<typename KeyValuePair, typename Compare, bool hasRank>
typename CompactingMap<KeyValuePair, Compare, hasRank>::TreeNode*
CompactingMap<KeyValuePair, Compare, hasRank>::lookupRank(int64_t ith) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::verifyRank() const
{
    if (!hasRank) {
        return true;
    }

    iterator it;
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
            iterator up = upperBound(k);
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

template<typename KeyValuePair, typename Compare, bool hasRank>
bool CompactingMap<KeyValuePair, Compare, hasRank>::verify() const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
int CompactingMap<KeyValuePair, Compare, hasRank>::inOrderCounterChecking(const TreeNode *n) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
int CompactingMap<KeyValuePair, Compare, hasRank>::verify(const TreeNode *n) const
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

template<typename KeyValuePair, typename Compare, bool hasRank>
int CompactingMap<KeyValuePair, Compare, hasRank>::fullCount(const TreeNode *n) const
{
    if (n == &NIL) {
        return 0;
    }
    return fullCount(n->left) + fullCount(n->right) + 1;
}

template<typename KeyValuePair, typename Compare, bool hasRank>
inline int
CompactingMap<KeyValuePair, Compare, hasRank>::compareKeyRegardlessOfPointer(const Key& key, TreeNode *node) const
{
    // assume key's pointer field is NULL, if there is a pointer field in key
    const void *tmp = node->kv.setPointerValue(NULL);
    int rv = m_comper(key, node->key());
    node->kv.setPointerValue(tmp);
    return rv;
}

} // namespace voltdb

#endif // COMPACTINGMAP_H_
