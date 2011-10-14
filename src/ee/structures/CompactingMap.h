/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef COMPACTINGMAP_H_
#define COMPACTINGMAP_H_

#include <cstdlib>
#include <utility>
#include <cassert>
#include "ContiguousAllocator.h"

namespace voltdb {

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

template<typename Key, typename Data, typename Compare>
class CompactingMap {
protected:
    static const char RED = 0;
    static const char BLACK = 1;

    struct TreeNode {
        Key key;
        Data value;
        TreeNode *parent;
        TreeNode *left;
        TreeNode *right;
        char color;
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
        friend class CompactingMap<Key, Data, Compare>;
    protected:
        const CompactingMap *m_map;
        TreeNode *m_node;
        iterator(const CompactingMap *m, TreeNode* x) : m_map(m), m_node(x) {}
    public:
        iterator() : m_map(NULL), m_node(NULL) {}
        iterator(const iterator &iter) : m_map(iter.m_map), m_node(iter.m_node) {}
        Key &key() const { return m_node->key; }
        Data &value() const { return m_node->value; }
        void setValue(const Data &value) { m_node->value = value; }
        void moveNext() { m_node = m_map->successor(m_node); }
        void movePrev() { m_node = m_map->predecessor(m_node); }
        bool isEnd() const { return ((!m_map) || (m_node == &(m_map->NIL))); }
        bool equals(iterator &iter) {
            if (isEnd()) return iter.isEnd();
            return m_node == iter.m_node;
        }
    };

    CompactingMap(bool unique, Compare comper);
    ~CompactingMap();

    bool insert(std::pair<Key, Data> value);
    bool erase(const Key &key);
    bool erase(iterator &iter);
    iterator find(const Key &key) { return iterator(this, lookup(key)); }
    int64_t size() const { return m_count; }
    iterator begin() const {
        if (!m_count) return iterator();
        return iterator(this, minimum(m_root));
    }
    iterator rbegin() const {
        if (!m_count) return iterator();
        return iterator(this, maximum(m_root));
    }

    iterator lowerBound(const Key &key);
    iterator upperBound(const Key &key);

    std::pair<iterator, iterator> equalRange(const Key &key);

    size_t bytesAllocated() const { return m_allocator.bytesAllocated(); }

    /**
     * For debugging: verify the RB-tree constraints are met. SLOW.
     */
    bool verify() const;

protected:
    // main internal functions
    void erase(TreeNode *z);
    TreeNode *lookup(const Key &key);

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
    int fullCount(const TreeNode *n) const;
};

template<typename Key, typename Data, typename Compare>
CompactingMap<Key, Data, Compare>::CompactingMap(bool unique, Compare comper)
    : m_count(0),
      m_root(&NIL),
      m_allocator(sizeof(TreeNode), 10000),
      m_unique(unique),
      m_comper(comper)
{
    NIL.left = NIL.right = NIL.parent = &NIL;
    NIL.color = BLACK;
}

template<typename Key, typename Data, typename Compare>
CompactingMap<Key, Data, Compare>::~CompactingMap() {
    iterator iter = begin();
    while (!iter.isEnd()) {
        iter.key().~Key();
        iter.value().~Data();
        iter.moveNext();
    }
}

template<typename Key, typename Data, typename Compare>
bool CompactingMap<Key, Data, Compare>::erase(const Key &key) {
    TreeNode *node = lookup(key);
    if (node == &NIL) return false;
    erase(node);
    return true;
}

template<typename Key, typename Data, typename Compare>
bool CompactingMap<Key, Data, Compare>::erase(iterator &iter) {
    assert(iter.m_node != &NIL);
    erase(iter.m_node);
    return true;
}

template<typename Key, typename Data, typename Compare>
bool CompactingMap<Key, Data, Compare>::insert(std::pair<Key, Data> value) {
    if (m_root != &NIL) {
        // find a place to put the new node
        TreeNode *y = &NIL;
        TreeNode *x = m_root;
        while (x != &NIL) {
            y = x;
            int cmp = m_comper(value.first, x->key);
            if (cmp < 0) x = x->left;
            else if (m_unique) {
                if (cmp == 0) return false;
                else x = x->right;
            }
            else x = x->right;
        }

        // create a new node
        void *memory = m_allocator.alloc();
        assert(memory);
        // placement new
        TreeNode *z = new(memory) TreeNode();
        z->key = value.first;
        z->value = value.second;
        z->left = z->right = &NIL;
        z->parent = y;
        z->color = RED;

        // stitch it in
        if (y == &NIL) m_root = z;
        else if (m_comper(z->key, y->key) < 0) y->left = z;
        else y->right = z;

        // rotate tree to balance if needed
        insertFixup(z);
    }
    else {
        // create a new node as root
        void *memory = m_allocator.alloc();
        assert(memory);
        // placement new
        TreeNode *z = new(memory) TreeNode();
        z->key = value.first;
        z->value = value.second;
        z->left = z->right = &NIL;
        z->parent = &NIL;
        z->color = BLACK;

        // make it root
        m_root = z;
    }
    m_count++;
    assert(m_allocator.count() == m_count);
    return true;
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::iterator CompactingMap<Key, Data, Compare>::lowerBound(const Key &key) {
    TreeNode *x = m_root;
    TreeNode *y = &NIL;
    while (x != &NIL) {
        int cmp = m_comper(x->key, key);
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

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::iterator CompactingMap<Key, Data, Compare>::upperBound(const Key &key) {
    TreeNode *x = m_root;
    TreeNode *y = &NIL;
    while (x != &NIL) {
        int cmp = m_comper(x->key, key);
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

template<typename Key, typename Data, typename Compare>
typename std::pair<typename CompactingMap<Key, Data, Compare>::iterator, typename CompactingMap<Key, Data, Compare>::iterator> CompactingMap<Key, Data, Compare>::equalRange(const Key &key) {
    return std::pair<iterator, iterator>(lowerBound(key), upperBound(key));
}

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::erase(TreeNode *z) {
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
        z->key = y->key;
        z->value = y->value;
        delnode = y;
    }

    if (y->color == BLACK)
        deleteFixup(x);
    m_count--;

    // move a node to fill this hole
    fragmentFixup(delnode);
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::TreeNode *CompactingMap<Key, Data, Compare>::lookup(const Key &key) {
    TreeNode *x = m_root;
    TreeNode *y = &NIL;
    TreeNode *retval = &NIL;
    while (x != &NIL) {
        int cmp = m_comper(x->key, key);
        if (cmp < 0) {
            x = x->right;
        }
        else if (cmp > 0) {
            y = x;
            x = x->left;
        }
        else if (cmp == 0) {
            retval = x;
            y = x;
            x = x->left;
        }
    }
    return retval;

}

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::leftRotate(TreeNode *x) {
    TreeNode *y = x->right;
    x->right = y->left;
    if (y->left != &NIL)
        y->left->parent = x;
    y->parent = x->parent;
    if (x->parent == &NIL)
        m_root = y;
    else if (x == x->parent->left)
        x->parent->left = y;
    else
        x->parent->right = y;
    y->left = x;
    x->parent = y;
}

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::rightRotate(TreeNode *x) {
    TreeNode *y = x->left;
    x->left = y->right;
    if (y->right != &NIL)
        y->right->parent = x;
    y->parent = x->parent;
    if (x->parent == &NIL)
        m_root = y;
    else if (x == x->parent->right)
        x->parent->right = y;
    else
        x->parent->left = y;
    y->right = x;
    x->parent = y;
}

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::insertFixup(TreeNode *z) {
    TreeNode *y;

    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            }
            else {
                if (z == z->parent->right)
                {
                    z = z->parent;
                    leftRotate(z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rightRotate(z->parent->parent);
            }
        }
        else {
            y = z->parent->parent->left;
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

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::deleteFixup(TreeNode *x) {
    TreeNode *w;

    while ((x != m_root) && (x->color == BLACK)) {
        if (x == x->parent->left) {
            w = x->parent->right;
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
            w = x->parent->left;
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

template<typename Key, typename Data, typename Compare>
void CompactingMap<Key, Data, Compare>::fragmentFixup(TreeNode *X) {
    // tree is empty now (after the recent delete)
    if (!m_count) {
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
    if (last->left != &NIL)
        last->left->parent = X;
    if (last->right != &NIL)
        last->right->parent = X;

    // copy the last node over the deleted node
    assert(X != &NIL);
    X->parent = last->parent;
    X->left = last->left;
    X->right = last->right;
    X->color = last->color;
    X->key = last->key;
    X->value = last->value;

    // fix the root pointer if needed
    if (last == m_root) {
        m_root = X;
    }

    last->~TreeNode();
    m_allocator.trim();
    assert(m_allocator.count() == m_count);
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::TreeNode *CompactingMap<Key, Data, Compare>::minimum(const TreeNode *subRoot) const {
    while (subRoot->left != &NIL) subRoot = subRoot->left;
    return const_cast<TreeNode*>(subRoot);
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::TreeNode *CompactingMap<Key, Data, Compare>::maximum(const TreeNode *subRoot) const {
    while (subRoot->right != &NIL) subRoot = subRoot->right;
    return const_cast<TreeNode*>(subRoot);
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::TreeNode *CompactingMap<Key, Data, Compare>::successor(const TreeNode *x) const {
    if (x->right != &NIL) return minimum(x->right);
    TreeNode *y = x->parent;
    while ((y != &NIL) && (x == y->right)) {
        x = y;
        y = y->parent;
    }
    return y;
}

template<typename Key, typename Data, typename Compare>
typename CompactingMap<Key, Data, Compare>::TreeNode *CompactingMap<Key, Data, Compare>::predecessor(const TreeNode *x) const {
    if (x->left != &NIL) return maximum(x->left);
    TreeNode *y = x->parent;
    while ((y != &NIL) && (x == y->left)) {
        x = y;
        y = y->parent;
    }
    return y;
}

template<typename Key, typename Data, typename Compare>
bool CompactingMap<Key, Data, Compare>::isReachableNode(const TreeNode* start, const TreeNode *dest) const {
    if (start == dest)
        return true;
    if ((start->left) && (isReachableNode(start->left, dest)))
        return true;
    if ((start->right) && (isReachableNode(start->right, dest)))
        return true;
    return false;
}

template<typename Key, typename Data, typename Compare>
bool CompactingMap<Key, Data, Compare>::verify() const {
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

    if (!m_root) return false;
    if ((m_root == &NIL) && (m_count)) return false;
    if (m_root->color == RED) return false;
    if (m_root->parent != &NIL) return false;
    if (verify(m_root) < 0) return false;
    if (m_count != fullCount(m_root)) return false;
    return true;
}

template<typename Key, typename Data, typename Compare>
int CompactingMap<Key, Data, Compare>::verify(const TreeNode *n) const {
    // recursive stopping case
    if (n == NULL) return false;
    if (n == &NIL)
        return 0;

    //printf("verify -> node %d\n", N->value);
    //fflush(stdout);

    // check children have a valid parent pointer
    if ((n->left != &NIL) && (n->left->parent != n)) return -1;
    if ((n->right != &NIL) && (n->right->parent != n)) return -1;

    // check for no two consecutive red nodes
    if (n->color == RED) {
        if ((n->left != &NIL) && (n->left->color == RED)) return -1;
        if ((n->right != &NIL) && (n->right->color == RED)) return -1;
    }

    // check for strict ordering
    if ((n->left != &NIL) && (m_comper(n->key, n->left->key) < 0)) return -1;
    if ((n->right != &NIL) && (m_comper(n->key, n->right->key) > 0)) return -1;

    // recursive step (compare black height)
    int leftBH = verify(n->left);
    int rightBH = verify(n->right);
    if (leftBH == -1) return -1;
    if (rightBH == -1) return -1;
    if (leftBH != rightBH) return -1;
    if (n->color == BLACK) return leftBH + 1;
    else return leftBH;
}

template<typename Key, typename Data, typename Compare>
int CompactingMap<Key, Data, Compare>::fullCount(const TreeNode *n) const {
    if (n == &NIL) return 0;
    else return fullCount(n->left) + fullCount(n->right) + 1;
}

} // namespace voltdb

#endif // COMPACTINGMAP_H_
