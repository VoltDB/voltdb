// $Id: btree.h 130 2011-05-18 08:24:25Z tb $ -*- fill-column: 79 -*-
/** \file btree.h
 * Contains the main B+ tree implementation template class btree.
 */

/*
 * STX B+ Tree Template Classes v0.8.6
 * Copyright (C) 2008-2011 Timo Bingmann
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or (at your
 * option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#ifndef _STX_BTREE_H_
#define _STX_BTREE_H_

// *** Required Headers from the STL

#include <algorithm>
#include <functional>
#include <istream>
#include <ostream>
#include <memory>
#include <cstddef>
#include <assert.h>

// *** Debugging Macros

#ifdef BTREE_DEBUG

#include <iostream>

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x)          do { if (debug) (std::cout << x); } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do { assert(x); } while(0)

#else

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x)          do { } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do { } while(0)

#endif

/// The maximum of a and b. Used in some compile-time formulas.
#define BTREE_MAX(a,b)          ((a) < (b) ? (b) : (a))

#ifndef BTREE_FRIENDS
/// The macro BTREE_FRIENDS can be used by outside class to access the B+
/// tree internals. This was added for wxBTreeDemo to be able to draw the
/// tree.
#define BTREE_FRIENDS           friend class btree_friend;
#endif

/// STX - Some Template Extensions namespace
namespace stx {

/** Generates default traits for a B+ tree used as a set. It estimates leaf and
 * inner node sizes by assuming a cache line size of 256 bytes. */
template <typename _Key>
struct btree_default_set_traits
{
    /// If true, the tree will self verify it's invariants after each insert()
    /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
    static const bool   selfverify = false;

    /// If true, the tree will print out debug information and a tree dump
    /// during insert() or erase() operation. The header must have been
    /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
    /// printable.
    static const bool   debug = false;

    /// Number of slots in each leaf of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int    leafslots = BTREE_MAX( 8, 256 / (sizeof(_Key)) );

    /// Number of slots in each inner node of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int    innerslots = BTREE_MAX( 8, 256 / (sizeof(_Key) + sizeof(void*)) );
};

/** Generates default traits for a B+ tree used as a map. It estimates leaf and
 * inner node sizes by assuming a cache line size of 256 bytes. */
template <typename _Key, typename _Data>
struct btree_default_map_traits
{
    /// If true, the tree will self verify it's invariants after each insert()
    /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
    static const bool   selfverify = false;

    /// If true, the tree will print out debug information and a tree dump
    /// during insert() or erase() operation. The header must have been
    /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
    /// printable.
    static const bool   debug = false;

    /// Number of slots in each leaf of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int    leafslots = BTREE_MAX( 8, 256 / (sizeof(_Key) + sizeof(_Data)) );

    /// Number of slots in each inner node of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int    innerslots = BTREE_MAX( 8, 256 / (sizeof(_Key) + sizeof(void*)) );
};

/** @brief Basic class implementing a base B+ tree data structure in memory.
 *
 * The base implementation of a memory B+ tree. It is based on the
 * implementation in Cormen's Introduction into Algorithms, Jan Jannink's paper
 * and other algorithm resources. Almost all STL-required function calls are
 * implemented. The asymptotic time requirements of the STL are not always
 * fulfilled in theory, however in practice this B+ tree performs better than a
 * red-black tree by using more memory. The insertion function splits the nodes
 * on the recursion unroll. Erase is largely based on Jannink's ideas.
 *
 * This class is specialized into btree_set, btree_multiset, btree_map and
 * btree_multimap using default template parameters and facade functions.
 */
template <typename _Key, typename _Data,
          typename _Value = std::pair<_Key, _Data>,
          typename _Compare = std::less<_Key>,
          typename _Traits = btree_default_map_traits<_Key, _Data>,
          bool _Duplicates = false,
          typename _Alloc = std::allocator<_Value> >
class btree
{
public:
    // *** Template Parameter Types

    /// First template parameter: The key type of the B+ tree. This is stored
    /// in inner nodes and leaves
    typedef _Key                        key_type;

    /// Second template parameter: The data type associated with each
    /// key. Stored in the B+ tree's leaves
    typedef _Data                       data_type;

    /// Third template parameter: Composition pair of key and data types, this
    /// is required by the STL standard. The B+ tree does not store key and
    /// data together. If value_type == key_type then the B+ tree implements a
    /// set.
    typedef _Value                      value_type;

    /// Fourth template parameter: Key comparison function object
    typedef _Compare                    key_compare;

    /// Fifth template parameter: Traits object used to define more parameters
    /// of the B+ tree
    typedef _Traits                     traits;

    /// Sixth template parameter: Allow duplicate keys in the B+ tree. Used to
    /// implement multiset and multimap.
    static const bool                   allow_duplicates = _Duplicates;

    /// Seventh template parameter: STL allocator for tree nodes
    typedef _Alloc                      allocator_type;

    // The macro BTREE_FRIENDS can be used by outside class to access the B+
    // tree internals. This was added for wxBTreeDemo to be able to draw the
    // tree.
    BTREE_FRIENDS

public:
    // *** Constructed Types

    /// Typedef of our own type
    typedef btree<key_type, data_type, value_type, key_compare,
		  traits, allow_duplicates, allocator_type> btree_self;

    /// Size type used to count keys
    typedef size_t                              size_type;

    /// The pair of key_type and data_type, this may be different from value_type.
    typedef std::pair<key_type, data_type>      pair_type;

public:
    // *** Static Constant Options and Values of the B+ Tree

    /// Base B+ tree parameter: The number of key/data slots in each leaf
    static const unsigned short         leafslotmax =  traits::leafslots;

    /// Base B+ tree parameter: The number of key slots in each inner node,
    /// this can differ from slots in each leaf.
    static const unsigned short         innerslotmax =  traits::innerslots;

    /// Computed B+ tree parameter: The minimum number of key/data slots used
    /// in a leaf. If fewer slots are used, the leaf will be merged or slots
    /// shifted from it's siblings.
    static const unsigned short minleafslots = (leafslotmax / 2);

    /// Computed B+ tree parameter: The minimum number of key slots used
    /// in an inner node. If fewer slots are used, the inner node will be
    /// merged or slots shifted from it's siblings.
    static const unsigned short mininnerslots = (innerslotmax / 2);

    /// Debug parameter: Enables expensive and thorough checking of the B+ tree
    /// invariants after each insert/erase operation.
    static const bool                   selfverify = traits::selfverify;

    /// Debug parameter: Prints out lots of debug information about how the
    /// algorithms change the tree. Requires the header file to be compiled
    /// with BTREE_DEBUG and the key type must be std::ostream printable.
    static const bool                   debug = traits::debug;

private:
    // *** Node Classes for In-Memory Nodes

    /// The header structure of each node in-memory. This structure is extended
    /// by inner_node or leaf_node.
    struct node
    {
        /// Level in the b-tree, if level == 0 -> leaf node
        unsigned short  level;

        /// Number of key slotuse use, so number of valid children or data
        /// pointers
        unsigned short  slotuse;

        /// Delayed initialisation of constructed node
        inline void initialize(const unsigned short l)
        {
            level = l;
            slotuse = 0;
        }

        /// True if this is a leaf node
        inline bool isleafnode() const
        {
            return (level == 0);
        }
    };

    /// Extended structure of a inner node in-memory. Contains only keys and no
    /// data items.
    struct inner_node : public node
    {
	/// Define an related allocator for the inner_node structs.
        typedef typename _Alloc::template rebind<inner_node>::other alloc_type;

        /// Keys of children or data pointers
        key_type        slotkey[innerslotmax];

        /// Pointers to children
        node*           childid[innerslotmax+1];

        /// Set variables to initial values
        inline void initialize(const unsigned short l)
        {
            node::initialize(l);
        }

        /// True if the node's slots are full
        inline bool isfull() const
        {
            return (node::slotuse == innerslotmax);
        }

        /// True if few used entries, less than half full
        inline bool isfew() const
        {
            return (node::slotuse <= mininnerslots);
        }

        /// True if node has too few entries
        inline bool isunderflow() const
        {
            return (node::slotuse < mininnerslots);
        }
    };

    /// Extended structure of a leaf node in memory. Contains pairs of keys and
    /// data items. Key and data slots are kept in separate arrays, because the
    /// key array is traversed very often compared to accessing the data items.
    struct leaf_node : public node
    {
	/// Define an related allocator for the leaf_node structs.
        typedef typename _Alloc::template rebind<leaf_node>::other alloc_type;

        /// Double linked list pointers to traverse the leaves
        leaf_node       *prevleaf;

        /// Double linked list pointers to traverse the leaves
        leaf_node       *nextleaf;

        /// Keys of children or data pointers
        key_type        slotkey[leafslotmax];

        /// Array of data
        data_type       slotdata[leafslotmax];

        /// Set variables to initial values
        inline void initialize()
        {
            node::initialize(0);
            prevleaf = nextleaf = NULL;
        }

        /// True if the node's slots are full
        inline bool isfull() const
        {
            return (node::slotuse == leafslotmax);
        }

        /// True if few used entries, less than half full
        inline bool isfew() const
        {
            return (node::slotuse <= minleafslots);
        }

        /// True if node has too few entries
        inline bool isunderflow() const
        {
            return (node::slotuse < minleafslots);
        }
    };

private:
    // *** Template Magic to Convert a pair or key/data types to a value_type

    /// For sets the second pair_type is an empty struct, so the value_type
    /// should only be the first.
    template <typename value_type, typename pair_type>
    struct btree_pair_to_value
    {
        /// Convert a fake pair type to just the first component
        inline value_type operator()(pair_type& p) const {
            return p.first;
        }
        /// Convert a fake pair type to just the first component
        inline value_type operator()(const pair_type& p) const {
            return p.first;
        }
    };

    /// For maps value_type is the same as the pair_type
    template <typename value_type>
    struct btree_pair_to_value<value_type, value_type>
    {
        /// Identity "convert" a real pair type to just the first component
        inline value_type operator()(pair_type& p) const {
            return p;
        }
        /// Identity "convert" a real pair type to just the first component
        inline value_type operator()(const pair_type& p) const {
            return p;
        }
    };

    /// Using template specialization select the correct converter used by the
    /// iterators
    typedef btree_pair_to_value<value_type, pair_type> pair_to_value_type;

public:
    // *** Iterators and Reverse Iterators

    class iterator;
    class const_iterator;
    class reverse_iterator;
    class const_reverse_iterator;

    /// STL-like iterator object for B+ tree items. The iterator points to a
    /// specific slot number in a leaf.
    class iterator
    {
    public:
        // *** Types

        /// The key type of the btree. Returned by key().
        typedef typename btree::key_type                key_type;

        /// The data type of the btree. Returned by data().
        typedef typename btree::data_type               data_type;

        /// The value type of the btree. Returned by operator*().
        typedef typename btree::value_type              value_type;

        /// The pair type of the btree.
        typedef typename btree::pair_type               pair_type;

        /// Reference to the value_type. STL required.
        typedef value_type&             reference;

        /// Pointer to the value_type. STL required.
        typedef value_type*             pointer;

        /// STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        /// STL-magic
        typedef ptrdiff_t               difference_type;

        /// Our own type
        typedef iterator                self;

    private:
        // *** Members

        /// The currently referenced leaf node of the tree
        typename btree::leaf_node*      currnode;

        /// Current key/data slot referenced
        unsigned short          currslot;

        /// Friendly to the const_iterator, so it may access the two data items directly.
        friend class const_iterator;

        /// Also friendly to the reverse_iterator, so it may access the two data items directly.
        friend class reverse_iterator;

        /// Also friendly to the const_reverse_iterator, so it may access the two data items directly.
        friend class const_reverse_iterator;

	/// Also friendly to the base btree class, because erase_iter() needs
	/// to read the currnode and currslot values directly.
	friend class btree<key_type, data_type, value_type, key_compare, traits, allow_duplicates>;

        /// Evil! A temporary value_type to STL-correctly deliver operator* and
        /// operator->
        mutable value_type              temp_value;

        // The macro BTREE_FRIENDS can be used by outside class to access the B+
        // tree internals. This was added for wxBTreeDemo to be able to draw the
        // tree.
        BTREE_FRIENDS

    public:
        // *** Methods

        /// Default-Constructor of a mutable iterator
        inline iterator()
            : currnode(NULL), currslot(0)
        { }

        /// Initializing-Constructor of a mutable iterator
        inline iterator(typename btree::leaf_node *l, unsigned short s)
            : currnode(l), currslot(s)
        { }

        /// Copy-constructor from a reverse iterator
        inline iterator(const reverse_iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Dereference the iterator, this is not a value_type& because key and
        /// value are not stored together
        inline reference operator*() const
        {
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot],
                                                         currnode->slotdata[currslot]) );
            return temp_value;
        }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline pointer operator->() const
        {
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot],
                                                         currnode->slotdata[currslot]) );
            return &temp_value;
        }

        /// Key of the current slot
        inline const key_type& key() const
        {
            return currnode->slotkey[currslot];
        }

        /// Writable reference to the current data object
        inline data_type& data() const
        {
            return currnode->slotdata[currslot];
        }

        /// Prefix++ advance the iterator to the next slot
        inline self& operator++()
        {
            if (currslot + 1 < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 0;
            }
            else {
                // this is end()
                currslot = currnode->slotuse;
            }

            return *this;
        }

        /// Postfix++ advance the iterator to the next slot
        inline self operator++(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot + 1 < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 0;
            }
            else {
                // this is end()
                currslot = currnode->slotuse;
            }

            return tmp;
        }

        /// Prefix-- backstep the iterator to the last slot
        inline self& operator--()
        {
            if (currslot > 0) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse - 1;
            }
            else {
                // this is begin()
                currslot = 0;
            }

            return *this;
        }

        /// Postfix-- backstep the iterator to the last slot
        inline self operator--(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot > 0) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse - 1;
            }
            else {
                // this is begin()
                currslot = 0;
            }

            return tmp;
        }

        /// Equality of iterators
        inline bool operator==(const self& x) const
        {
            return (x.currnode == currnode) && (x.currslot == currslot);
        }

        /// Inequality of iterators
        inline bool operator!=(const self& x) const
        {
            return (x.currnode != currnode) || (x.currslot != currslot);
        }
    };

    /// STL-like read-only iterator object for B+ tree items. The iterator
    /// points to a specific slot number in a leaf.
    class const_iterator
    {
    public:
        // *** Types

        /// The key type of the btree. Returned by key().
        typedef typename btree::key_type                key_type;

        /// The data type of the btree. Returned by data().
        typedef typename btree::data_type               data_type;

        /// The value type of the btree. Returned by operator*().
        typedef typename btree::value_type              value_type;

        /// The pair type of the btree.
        typedef typename btree::pair_type               pair_type;

        /// Reference to the value_type. STL required.
        typedef const value_type&               reference;

        /// Pointer to the value_type. STL required.
        typedef const value_type*               pointer;

        /// STL-magic iterator category
        typedef std::bidirectional_iterator_tag         iterator_category;

        /// STL-magic
        typedef ptrdiff_t               difference_type;

        /// Our own type
        typedef const_iterator          self;

    private:
        // *** Members

        /// The currently referenced leaf node of the tree
        const typename btree::leaf_node*        currnode;

        /// Current key/data slot referenced
        unsigned short                  currslot;

        /// Friendly to the reverse_const_iterator, so it may access the two data items directly
        friend class const_reverse_iterator;

        /// Evil! A temporary value_type to STL-correctly deliver operator* and
        /// operator->
        mutable value_type              temp_value;

        // The macro BTREE_FRIENDS can be used by outside class to access the B+
        // tree internals. This was added for wxBTreeDemo to be able to draw the
        // tree.
        BTREE_FRIENDS

    public:
        // *** Methods

        /// Default-Constructor of a const iterator
        inline const_iterator()
            : currnode(NULL), currslot(0)
        { }

        /// Initializing-Constructor of a const iterator
        inline const_iterator(const typename btree::leaf_node *l, unsigned short s)
            : currnode(l), currslot(s)
        { }

        /// Copy-constructor from a mutable iterator
        inline const_iterator(const iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Copy-constructor from a mutable reverse iterator
        inline const_iterator(const reverse_iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Copy-constructor from a const reverse iterator
        inline const_iterator(const const_reverse_iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline reference operator*() const
        {
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot],
                                                         currnode->slotdata[currslot]) );
            return temp_value;
        }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline pointer operator->() const
        {
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot],
                                                         currnode->slotdata[currslot]) );
            return &temp_value;
        }

        /// Key of the current slot
        inline const key_type& key() const
        {
            return currnode->slotkey[currslot];
        }

        /// Read-only reference to the current data object
        inline const data_type& data() const
        {
            return currnode->slotdata[currslot];
        }

        /// Prefix++ advance the iterator to the next slot
        inline self& operator++()
        {
            if (currslot + 1 < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 0;
            }
            else {
                // this is end()
                currslot = currnode->slotuse;
            }

            return *this;
        }

        /// Postfix++ advance the iterator to the next slot
        inline self operator++(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot + 1 < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 0;
            }
            else {
                // this is end()
                currslot = currnode->slotuse;
            }

            return tmp;
        }

        /// Prefix-- backstep the iterator to the last slot
        inline self& operator--()
        {
            if (currslot > 0) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse - 1;
            }
            else {
                // this is begin()
                currslot = 0;
            }

            return *this;
        }

        /// Postfix-- backstep the iterator to the last slot
        inline self operator--(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot > 0) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse - 1;
            }
            else {
                // this is begin()
                currslot = 0;
            }

            return tmp;
        }

        /// Equality of iterators
        inline bool operator==(const self& x) const
        {
            return (x.currnode == currnode) && (x.currslot == currslot);
        }

        /// Inequality of iterators
        inline bool operator!=(const self& x) const
        {
            return (x.currnode != currnode) || (x.currslot != currslot);
        }
    };

    /// STL-like mutable reverse iterator object for B+ tree items. The
    /// iterator points to a specific slot number in a leaf.
    class reverse_iterator
    {
    public:
        // *** Types

        /// The key type of the btree. Returned by key().
        typedef typename btree::key_type                key_type;

        /// The data type of the btree. Returned by data().
        typedef typename btree::data_type               data_type;

        /// The value type of the btree. Returned by operator*().
        typedef typename btree::value_type              value_type;

        /// The pair type of the btree.
        typedef typename btree::pair_type               pair_type;

        /// Reference to the value_type. STL required.
        typedef value_type&             reference;

        /// Pointer to the value_type. STL required.
        typedef value_type*             pointer;

        /// STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        /// STL-magic
        typedef ptrdiff_t               difference_type;

        /// Our own type
        typedef reverse_iterator        self;

    private:
        // *** Members

        /// The currently referenced leaf node of the tree
        typename btree::leaf_node*      currnode;

        /// One slot past the current key/data slot referenced.
        unsigned short          currslot;

        /// Friendly to the const_iterator, so it may access the two data items directly
        friend class iterator;

        /// Also friendly to the const_iterator, so it may access the two data items directly
        friend class const_iterator;

        /// Also friendly to the const_iterator, so it may access the two data items directly
        friend class const_reverse_iterator;

        /// Evil! A temporary value_type to STL-correctly deliver operator* and
        /// operator->
        mutable value_type              temp_value;

        // The macro BTREE_FRIENDS can be used by outside class to access the B+
        // tree internals. This was added for wxBTreeDemo to be able to draw the
        // tree.
        BTREE_FRIENDS

    public:
        // *** Methods

        /// Default-Constructor of a reverse iterator
        inline reverse_iterator()
            : currnode(NULL), currslot(0)
        { }

        /// Initializing-Constructor of a mutable reverse iterator
        inline reverse_iterator(typename btree::leaf_node *l, unsigned short s)
            : currnode(l), currslot(s)
        { }

        /// Copy-constructor from a mutable iterator
        inline reverse_iterator(const iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Dereference the iterator, this is not a value_type& because key and
        /// value are not stored together
        inline reference operator*() const
        {
            BTREE_ASSERT(currslot > 0);
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot - 1],
                                                         currnode->slotdata[currslot - 1]) );
            return temp_value;
        }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline pointer operator->() const
        {
            BTREE_ASSERT(currslot > 0);
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot - 1],
                                                         currnode->slotdata[currslot - 1]) );
            return &temp_value;
        }

        /// Key of the current slot
        inline const key_type& key() const
        {
            BTREE_ASSERT(currslot > 0);
            return currnode->slotkey[currslot - 1];
        }

        /// Writable reference to the current data object
        inline data_type& data() const
        {
            BTREE_ASSERT(currslot > 0);
            return currnode->slotdata[currslot - 1];
        }

        /// Prefix++ advance the iterator to the next slot
        inline self& operator++()
        {
            if (currslot > 1) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse;
            }
            else {
                // this is begin() == rend()
                currslot = 0;
            }

            return *this;
        }

        /// Postfix++ advance the iterator to the next slot
        inline self operator++(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot > 1) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse;
            }
            else {
                // this is begin() == rend()
                currslot = 0;
            }

            return tmp;
        }

        /// Prefix-- backstep the iterator to the last slot
        inline self& operator--()
        {
            if (currslot < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 1;
            }
            else {
                // this is end() == rbegin()
                currslot = currnode->slotuse;
            }

            return *this;
        }

        /// Postfix-- backstep the iterator to the last slot
        inline self operator--(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 1;
            }
            else {
                // this is end() == rbegin()
                currslot = currnode->slotuse;
            }

            return tmp;
        }

        /// Equality of iterators
        inline bool operator==(const self& x) const
        {
            return (x.currnode == currnode) && (x.currslot == currslot);
        }

        /// Inequality of iterators
        inline bool operator!=(const self& x) const
        {
            return (x.currnode != currnode) || (x.currslot != currslot);
        }
    };

    /// STL-like read-only reverse iterator object for B+ tree items. The
    /// iterator points to a specific slot number in a leaf.
    class const_reverse_iterator
    {
    public:
        // *** Types

        /// The key type of the btree. Returned by key().
        typedef typename btree::key_type                key_type;

        /// The data type of the btree. Returned by data().
        typedef typename btree::data_type               data_type;

        /// The value type of the btree. Returned by operator*().
        typedef typename btree::value_type              value_type;

        /// The pair type of the btree.
        typedef typename btree::pair_type               pair_type;

        /// Reference to the value_type. STL required.
        typedef const value_type&               reference;

        /// Pointer to the value_type. STL required.
        typedef const value_type*               pointer;

        /// STL-magic iterator category
        typedef std::bidirectional_iterator_tag         iterator_category;

        /// STL-magic
        typedef ptrdiff_t               difference_type;

        /// Our own type
        typedef const_reverse_iterator  self;

    private:
        // *** Members

        /// The currently referenced leaf node of the tree
        const typename btree::leaf_node*        currnode;

        /// One slot past the current key/data slot referenced.
        unsigned short                          currslot;

        /// Friendly to the const_iterator, so it may access the two data items directly.
        friend class reverse_iterator;

        /// Evil! A temporary value_type to STL-correctly deliver operator* and
        /// operator->
        mutable value_type              temp_value;

        // The macro BTREE_FRIENDS can be used by outside class to access the B+
        // tree internals. This was added for wxBTreeDemo to be able to draw the
        // tree.
        BTREE_FRIENDS

    public:
        // *** Methods

        /// Default-Constructor of a const reverse iterator
        inline const_reverse_iterator()
            : currnode(NULL), currslot(0)
        { }

        /// Initializing-Constructor of a const reverse iterator
        inline const_reverse_iterator(const typename btree::leaf_node *l, unsigned short s)
            : currnode(l), currslot(s)
        { }

        /// Copy-constructor from a mutable iterator
        inline const_reverse_iterator(const iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Copy-constructor from a const iterator
        inline const_reverse_iterator(const const_iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Copy-constructor from a mutable reverse iterator
        inline const_reverse_iterator(const reverse_iterator &it)
            : currnode(it.currnode), currslot(it.currslot)
        { }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline reference operator*() const
        {
            BTREE_ASSERT(currslot > 0);
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot - 1],
                                                         currnode->slotdata[currslot - 1]) );
            return temp_value;
        }

        /// Dereference the iterator. Do not use this if possible, use key()
        /// and data() instead. The B+ tree does not stored key and data
        /// together.
        inline pointer operator->() const
        {
            BTREE_ASSERT(currslot > 0);
            temp_value = pair_to_value_type()( pair_type(currnode->slotkey[currslot - 1],
                                                         currnode->slotdata[currslot - 1]) );
            return &temp_value;
        }

        /// Key of the current slot
        inline const key_type& key() const
        {
            BTREE_ASSERT(currslot > 0);
            return currnode->slotkey[currslot - 1];
        }

        /// Read-only reference to the current data object
        inline const data_type& data() const
        {
            BTREE_ASSERT(currslot > 0);
            return currnode->slotdata[currslot - 1];
        }

        /// Prefix++ advance the iterator to the previous slot
        inline self& operator++()
        {
            if (currslot > 1) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse;
            }
            else {
                // this is begin() == rend()
                currslot = 0;
            }

            return *this;
        }

        /// Postfix++ advance the iterator to the previous slot
        inline self operator++(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot > 1) {
                --currslot;
            }
            else if (currnode->prevleaf != NULL) {
                currnode = currnode->prevleaf;
                currslot = currnode->slotuse;
            }
            else {
                // this is begin() == rend()
                currslot = 0;
            }

            return tmp;
        }

        /// Prefix-- backstep the iterator to the next slot
        inline self& operator--()
        {
            if (currslot < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 1;
            }
            else {
                // this is end() == rbegin()
                currslot = currnode->slotuse;
            }

            return *this;
        }

        /// Postfix-- backstep the iterator to the next slot
        inline self operator--(int)
        {
            self tmp = *this;   // copy ourselves

            if (currslot < currnode->slotuse) {
                ++currslot;
            }
            else if (currnode->nextleaf != NULL) {
                currnode = currnode->nextleaf;
                currslot = 1;
            }
            else {
                // this is end() == rbegin()
                currslot = currnode->slotuse;
            }

            return tmp;
        }

        /// Equality of iterators
        inline bool operator==(const self& x) const
        {
            return (x.currnode == currnode) && (x.currslot == currslot);
        }

        /// Inequality of iterators
        inline bool operator!=(const self& x) const
        {
            return (x.currnode != currnode) || (x.currslot != currslot);
        }
    };

public:
    // *** Small Statistics Structure

    /** A small struct containing basic statistics about the B+ tree. It can be
     * fetched using get_stats(). */
    struct tree_stats
    {
        /// Number of items in the B+ tree
        size_type       itemcount;

        /// Number of leaves in the B+ tree
        size_type       leaves;

        /// Number of inner nodes in the B+ tree
        size_type       innernodes;

        /// Base B+ tree parameter: The number of key/data slots in each leaf
        static const unsigned short     leafslots = btree_self::leafslotmax;

        /// Base B+ tree parameter: The number of key slots in each inner node.
        static const unsigned short     innerslots = btree_self::innerslotmax;

        /// Zero initialized
        inline tree_stats()
            : itemcount(0),
              leaves(0), innernodes(0)
        {
        }

        /// Return the total number of nodes
        inline size_type nodes() const
        {
            return innernodes + leaves;
        }

        /// Return the average fill of leaves
        inline double avgfill_leaves() const
        {
            return static_cast<double>(itemcount) / (leaves * leafslots);
        }
    };

private:
    // *** Tree Object Data Members

    /// Pointer to the B+ tree's root node, either leaf or inner node
    node*       root;

    /// Pointer to first leaf in the double linked leaf chain
    leaf_node   *headleaf;

    /// Pointer to last leaf in the double linked leaf chain
    leaf_node   *tailleaf;

    /// Other small statistics about the B+ tree
    tree_stats  stats;

    /// Key comparison object. More comparison functions are generated from
    /// this < relation.
    key_compare key_less;

    /// Memory allocator.
    allocator_type allocator;

public:
    // *** Constructors and Destructor

    /// Default constructor initializing an empty B+ tree with the standard key
    /// comparison function
    explicit inline btree(const allocator_type &alloc = allocator_type())
        : root(NULL), headleaf(NULL), tailleaf(NULL), allocator(alloc)
    {
    }

    /// Constructor initializing an empty B+ tree with a special key
    /// comparison object
    explicit inline btree(const key_compare &kcf,
                          const allocator_type &alloc = allocator_type())
        : root(NULL), headleaf(NULL), tailleaf(NULL),
          key_less(kcf), allocator(alloc)
    {
    }

    /// Constructor initializing a B+ tree with the range [first,last)
    template <class InputIterator>
    inline btree(InputIterator first, InputIterator last,
                 const allocator_type &alloc = allocator_type())
        : root(NULL), headleaf(NULL), tailleaf(NULL), allocator(alloc)
    {
        insert(first, last);
    }

    /// Constructor initializing a B+ tree with the range [first,last) and a
    /// special key comparison object
    template <class InputIterator>
    inline btree(InputIterator first, InputIterator last, const key_compare &kcf,
                 const allocator_type &alloc = allocator_type())
        : root(NULL), headleaf(NULL), tailleaf(NULL),
          key_less(kcf), allocator(alloc)
    {
        insert(first, last);
    }

    /// Frees up all used B+ tree memory pages
    inline ~btree()
    {
        clear();
    }

    /// Fast swapping of two identical B+ tree objects.
    void swap(btree_self& from)
    {
        std::swap(root, from.root);
        std::swap(headleaf, from.headleaf);
        std::swap(tailleaf, from.tailleaf);
        std::swap(stats, from.stats);
        std::swap(key_less, from.key_less);
        std::swap(allocator, from.allocator);
    }

public:
    // *** Key and Value Comparison Function Objects

    /// Function class to compare value_type objects. Required by the STL
    class value_compare
    {
    protected:
        /// Key comparison function from the template parameter
        key_compare     key_comp;

        /// Constructor called from btree::value_comp()
        inline value_compare(key_compare kc)
            : key_comp(kc)
        { }

        /// Friendly to the btree class so it may call the constructor
        friend class btree<key_type, data_type, value_type, key_compare, traits, allow_duplicates>;

    public:
        /// Function call "less"-operator resulting in true if x < y.
        inline bool operator()(const value_type& x, const value_type& y) const
        {
            return key_comp(x.first, y.first);
        }
    };

    /// Constant access to the key comparison object sorting the B+ tree
    inline key_compare key_comp() const
    {
        return key_less;
    }

    /// Constant access to a constructed value_type comparison object. Required
    /// by the STL
    inline value_compare value_comp() const
    {
        return value_compare(key_less);
    }

private:
    // *** Convenient Key Comparison Functions Generated From key_less

    /// True if a <= b ? constructed from key_less()
    inline bool key_lessequal(const key_type &a, const key_type b) const
    {
        return !key_less(b, a);
    }

    /// True if a > b ? constructed from key_less()
    inline bool key_greater(const key_type &a, const key_type &b) const
    {
        return key_less(b, a);
    }

    /// True if a >= b ? constructed from key_less()
    inline bool key_greaterequal(const key_type &a, const key_type b) const
    {
        return !key_less(a, b);
    }

    /// True if a == b ? constructed from key_less(). This requires the <
    /// relation to be a total order, otherwise the B+ tree cannot be sorted.
    inline bool key_equal(const key_type &a, const key_type &b) const
    {
        return !key_less(a, b) && !key_less(b, a);
    }

public:
    // *** Allocators

    /// Return the base node allocator provided during construction.
    allocator_type get_allocator() const
    {
        return allocator;
    }

private:
    // *** Node Object Allocation and Deallocation Functions

    /// Return an allocator for leaf_node objects
    typename leaf_node::alloc_type leaf_node_allocator()
    {
        return typename leaf_node::alloc_type(allocator);
    }

    /// Return an allocator for inner_node objects
    typename inner_node::alloc_type inner_node_allocator()
    {
        return typename inner_node::alloc_type(allocator);
    }

    /// Allocate and initialize a leaf node
    inline leaf_node* allocate_leaf()
    {
        leaf_node *n = new (leaf_node_allocator().allocate(1)) leaf_node();
        n->initialize();
        stats.leaves++;
        return n;
    }

    /// Allocate and initialize an inner node
    inline inner_node* allocate_inner(unsigned short level)
    {
        inner_node *n = new (inner_node_allocator().allocate(1)) inner_node();
        n->initialize(level);
        stats.innernodes++;
        return n;
    }

    /// Correctly free either inner or leaf node, destructs all contained key
    /// and value objects
    inline void free_node(node *n)
    {
        if (n->isleafnode()) {
            leaf_node *ln = static_cast<leaf_node*>(n);
            typename leaf_node::alloc_type a(leaf_node_allocator());
            a.destroy(ln);
            a.deallocate(ln, 1);
            stats.leaves--;
        }
        else {
            inner_node *in = static_cast<inner_node*>(n);
            typename inner_node::alloc_type a(inner_node_allocator());
            a.destroy(in);
            a.deallocate(in, 1);
            stats.innernodes--;
        }
    }

public:
    // *** Fast Destruction of the B+ Tree

    /// Frees all key/data pairs and all nodes of the tree
    void clear()
    {
        if (root)
        {
            clear_recursive(root);
            free_node(root);

            root = NULL;
            headleaf = tailleaf = NULL;

            stats = tree_stats();
        }

        BTREE_ASSERT(stats.itemcount == 0);
    }

private:
    /// Recursively free up nodes
    void clear_recursive(node *n)
    {
        if (n->isleafnode())
        {
            leaf_node *leafnode = static_cast<leaf_node*>(n);

            for (unsigned int slot = 0; slot < leafnode->slotuse; ++slot)
            {
                // data objects are deleted by leaf_node's destructor
            }
        }
        else
        {
            inner_node *innernode = static_cast<inner_node*>(n);

            for (unsigned short slot = 0; slot < innernode->slotuse + 1; ++slot)
            {
                clear_recursive(innernode->childid[slot]);
                free_node(innernode->childid[slot]);
            }
        }
    }

public:
    // *** STL Iterator Construction Functions

    /// Constructs a read/data-write iterator that points to the first slot in
    /// the first leaf of the B+ tree.
    inline iterator begin()
    {
        return iterator(headleaf, 0);
    }

    /// Constructs a read/data-write iterator that points to the first invalid
    /// slot in the last leaf of the B+ tree.
    inline iterator end()
    {
        return iterator(tailleaf, tailleaf ? tailleaf->slotuse : 0);
    }

    /// Constructs a read-only constant iterator that points to the first slot
    /// in the first leaf of the B+ tree.
    inline const_iterator begin() const
    {
        return const_iterator(headleaf, 0);
    }

    /// Constructs a read-only constant iterator that points to the first
    /// invalid slot in the last leaf of the B+ tree.
    inline const_iterator end() const
    {
        return const_iterator(tailleaf, tailleaf ? tailleaf->slotuse : 0);
    }

    /// Constructs a read/data-write reverse iterator that points to the first
    /// invalid slot in the last leaf of the B+ tree. Uses STL magic.
    inline reverse_iterator rbegin()
    {
        return reverse_iterator(end());
    }

    /// Constructs a read/data-write reverse iterator that points to the first
    /// slot in the first leaf of the B+ tree. Uses STL magic.
    inline reverse_iterator rend()
    {
        return reverse_iterator(begin());
    }

    /// Constructs a read-only reverse iterator that points to the first
    /// invalid slot in the last leaf of the B+ tree. Uses STL magic.
    inline const_reverse_iterator rbegin() const
    {
        return const_reverse_iterator(end());
    }

    /// Constructs a read-only reverse iterator that points to the first slot
    /// in the first leaf of the B+ tree. Uses STL magic.
    inline const_reverse_iterator rend() const
    {
        return const_reverse_iterator(begin());
    }

private:
    // *** B+ Tree Node Binary Search Functions

    /// Searches for the first key in the node n less or equal to key. Uses
    /// binary search with an optional linear self-verification. This is a
    /// template function, because the slotkey array is located at different
    /// places in leaf_node and inner_node.
    template <typename node_type>
    inline int find_lower(const node_type *n, const key_type& key) const
    {
        if (n->slotuse == 0) return 0;

        int lo = 0,
            hi = n->slotuse - 1;

        while(lo < hi)
        {
            int mid = (lo + hi) >> 1;

            if (key_lessequal(key, n->slotkey[mid])) {
                hi = mid - 1;
            }
            else {
                lo = mid + 1;
            }
        }

        if (hi < 0 || key_less(n->slotkey[hi], key))
            hi++;

        BTREE_PRINT("btree::find_lower: on " << n << " key " << key << " -> (" << lo << ") " << hi << ", ");

        // verify result using simple linear search
        if (selfverify)
        {
            int i = n->slotuse - 1;
            while(i >= 0 && key_lessequal(key, n->slotkey[i]))
                i--;
            i++;

            BTREE_PRINT("testfind: " << i << std::endl);
            BTREE_ASSERT(i == hi);
        }
        else {
            BTREE_PRINT(std::endl);
        }

        return hi;
    }

    /// Searches for the first key in the node n greater than key. Uses binary
    /// search with an optional linear self-verification. This is a template
    /// function, because the slotkey array is located at different places in
    /// leaf_node and inner_node.
    template <typename node_type>
    inline int find_upper(const node_type *n, const key_type& key) const
    {
        if (n->slotuse == 0) return 0;

        int lo = 0,
            hi = n->slotuse - 1;

        while(lo < hi)
        {
            int mid = (lo + hi) >> 1;

            if (key_less(key, n->slotkey[mid])) {
                hi = mid - 1;
            }
            else {
                lo = mid + 1;
            }
        }

        if (hi < 0 || key_lessequal(n->slotkey[hi], key))
            hi++;

        BTREE_PRINT("btree::find_upper: on " << n << " key " << key << " -> (" << lo << ") " << hi << ", ");

        // verify result using simple linear search
        if (selfverify)
        {
            int i = n->slotuse - 1;
            while(i >= 0 && key_less(key, n->slotkey[i]))
                i--;
            i++;

            BTREE_PRINT("btree::find_upper testfind: " << i << std::endl);
            BTREE_ASSERT(i == hi);
        }
        else {
            BTREE_PRINT(std::endl);
        }

        return hi;
    }

public:
    // *** Access Functions to the Item Count

    /// Return the number of key/data pairs in the B+ tree
    inline size_type size() const
    {
        return stats.itemcount;
    }

    /// Returns true if there is at least one key/data pair in the B+ tree
    inline bool empty() const
    {
        return (size() == size_type(0));
    }

    /// Returns the largest possible size of the B+ Tree. This is just a
    /// function required by the STL standard, the B+ Tree can hold more items.
    inline size_type max_size() const
    {
        return size_type(-1);
    }

    /// Return a const reference to the current statistics.
    inline const struct tree_stats& get_stats() const
    {
        return stats;
    }

public:
    // *** Standard Access Functions Querying the Tree by Descending to a Leaf

    /// Non-STL function checking whether a key is in the B+ tree. The same as
    /// (find(k) != end()) or (count() != 0).
    bool exists(const key_type &key) const
    {
        const node *n = root;
        if (!n) return false;

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        const leaf_node *leaf = static_cast<const leaf_node*>(n);

        int slot = find_lower(leaf, key);
        return (slot < leaf->slotuse && key_equal(key, leaf->slotkey[slot]));
    }

    /// Tries to locate a key in the B+ tree and returns an iterator to the
    /// key/data slot if found. If unsuccessful it returns end().
    iterator find(const key_type &key)
    {
        node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        leaf_node *leaf = static_cast<leaf_node*>(n);

        int slot = find_lower(leaf, key);
        return (slot < leaf->slotuse && key_equal(key, leaf->slotkey[slot]))
            ? iterator(leaf, slot) : end();
    }

    /// Tries to locate a key in the B+ tree and returns an constant iterator
    /// to the key/data slot if found. If unsuccessful it returns end().
    const_iterator find(const key_type &key) const
    {
        const node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        const leaf_node *leaf = static_cast<const leaf_node*>(n);

        int slot = find_lower(leaf, key);
        return (slot < leaf->slotuse && key_equal(key, leaf->slotkey[slot]))
            ? const_iterator(leaf, slot) : end();
    }

    /// Tries to locate a key in the B+ tree and returns the number of
    /// identical key entries found.
    size_type count(const key_type &key) const
    {
        const node *n = root;
        if (!n) return 0;

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        const leaf_node *leaf = static_cast<const leaf_node*>(n);

        int slot = find_lower(leaf, key);
        size_type num = 0;

        while (leaf && slot < leaf->slotuse && key_equal(key, leaf->slotkey[slot]))
        {
            ++num;
            if (++slot >= leaf->slotuse)
            {
                leaf = leaf->nextleaf;
                slot = 0;
            }
        }

        return num;
    }

    /// Searches the B+ tree and returns an iterator to the first pair
    /// equal to or greater than key, or end() if all keys are smaller.
    iterator lower_bound(const key_type& key)
    {
        node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        leaf_node *leaf = static_cast<leaf_node*>(n);

        int slot = find_lower(leaf, key);
        return iterator(leaf, slot);
    }

    /// Searches the B+ tree and returns a constant iterator to the
    /// first pair equal to or greater than key, or end() if all keys
    /// are smaller.
    const_iterator lower_bound(const key_type& key) const
    {
        const node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_lower(inner, key);

            n = inner->childid[slot];
        }

        const leaf_node *leaf = static_cast<const leaf_node*>(n);

        int slot = find_lower(leaf, key);
        return const_iterator(leaf, slot);
    }

    /// Searches the B+ tree and returns an iterator to the first pair
    /// greater than key, or end() if all keys are smaller or equal.
    iterator upper_bound(const key_type& key)
    {
        node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_upper(inner, key);

            n = inner->childid[slot];
        }

        leaf_node *leaf = static_cast<leaf_node*>(n);

        int slot = find_upper(leaf, key);
        return iterator(leaf, slot);
    }

    /// Searches the B+ tree and returns a constant iterator to the
    /// first pair greater than key, or end() if all keys are smaller
    /// or equal.
    const_iterator upper_bound(const key_type& key) const
    {
        const node *n = root;
        if (!n) return end();

        while(!n->isleafnode())
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            int slot = find_upper(inner, key);

            n = inner->childid[slot];
        }

        const leaf_node *leaf = static_cast<const leaf_node*>(n);

        int slot = find_upper(leaf, key);
        return const_iterator(leaf, slot);
    }

    /// Searches the B+ tree and returns both lower_bound() and upper_bound().
    inline std::pair<iterator, iterator> equal_range(const key_type& key)
    {
        return std::pair<iterator, iterator>(lower_bound(key), upper_bound(key));
    }

    /// Searches the B+ tree and returns both lower_bound() and upper_bound().
    inline std::pair<const_iterator, const_iterator> equal_range(const key_type& key) const
    {
        return std::pair<const_iterator, const_iterator>(lower_bound(key), upper_bound(key));
    }

public:
    // *** B+ Tree Object Comparison Functions

    /// Equality relation of B+ trees of the same type. B+ trees of the same
    /// size and equal elements (both key and data) are considered
    /// equal. Beware of the random ordering of duplicate keys.
    inline bool operator==(const btree_self &other) const
    {
        return (size() == other.size()) && std::equal(begin(), end(), other.begin());
    }

    /// Inequality relation. Based on operator==.
    inline bool operator!=(const btree_self &other) const
    {
        return !(*this == other);
    }

    /// Total ordering relation of B+ trees of the same type. It uses
    /// std::lexicographical_compare() for the actual comparison of elements.
    inline bool operator<(const btree_self &other) const
    {
        return std::lexicographical_compare(begin(), end(), other.begin(), other.end());
    }

    /// Greater relation. Based on operator<.
    inline bool operator>(const btree_self &other) const
    {
        return other < *this;
    }

    /// Less-equal relation. Based on operator<.
    inline bool operator<=(const btree_self &other) const
    {
        return !(other < *this);
    }

    /// Greater-equal relation. Based on operator<.
    inline bool operator>=(const btree_self &other) const
    {
        return !(*this < other);
    }

public:
    /// *** Fast Copy: Assign Operator and Copy Constructors

    /// Assignment operator. All the key/data pairs are copied
    inline btree_self& operator= (const btree_self &other)
    {
        if (this != &other)
        {
            clear();

            key_less = other.key_comp();
            allocator = other.get_allocator();

            if (other.size() != 0)
            {
                stats.leaves = stats.innernodes = 0;
                if (other.root) {
                    root = copy_recursive(other.root);
                }
                stats = other.stats;
            }

            if (selfverify) verify();
        }
        return *this;
    }

    /// Copy constructor. The newly initialized B+ tree object will contain a
    /// copy of all key/data pairs.
    inline btree(const btree_self &other)
        : root(NULL), headleaf(NULL), tailleaf(NULL),
          stats( other.stats ),
          key_less( other.key_comp() ),
          allocator( other.get_allocator() )
    {
        if (size() > 0)
        {
            stats.leaves = stats.innernodes = 0;
            if (other.root) {
                root = copy_recursive(other.root);
            }
            if (selfverify) verify();
        }
    }

private:
    /// Recursively copy nodes from another B+ tree object
    struct node* copy_recursive(const node *n)
    {
        if (n->isleafnode())
        {
            const leaf_node *leaf = static_cast<const leaf_node*>(n);
            leaf_node *newleaf = allocate_leaf();

            newleaf->slotuse = leaf->slotuse;
            std::copy(leaf->slotkey, leaf->slotkey + leaf->slotuse, newleaf->slotkey);
            std::copy(leaf->slotdata, leaf->slotdata + leaf->slotuse, newleaf->slotdata);

            if (headleaf == NULL)
            {
                headleaf = tailleaf = newleaf;
                newleaf->prevleaf = newleaf->nextleaf = NULL;
            }
            else
            {
                newleaf->prevleaf = tailleaf;
                tailleaf->nextleaf = newleaf;
                tailleaf = newleaf;
            }

            return newleaf;
        }
        else
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            inner_node *newinner = allocate_inner(inner->level);

            newinner->slotuse = inner->slotuse;
            std::copy(inner->slotkey, inner->slotkey + inner->slotuse, newinner->slotkey);

            for (unsigned short slot = 0; slot <= inner->slotuse; ++slot)
            {
                newinner->childid[slot] = copy_recursive(inner->childid[slot]);
            }

            return newinner;
        }
    }

public:
    // *** Public Insertion Functions

    /// Attempt to insert a key/data pair into the B+ tree. If the tree does not
    /// allow duplicate keys, then the insert may fail if it is already
    /// present.
    inline std::pair<iterator, bool> insert(const pair_type& x)
    {
        return insert_start(x.first, x.second);
    }

    /// Attempt to insert a key/data pair into the B+ tree. Beware that if
    /// key_type == data_type, then the template iterator insert() is called
    /// instead. If the tree does not allow duplicate keys, then the insert may
    /// fail if it is already present.
    inline std::pair<iterator, bool> insert(const key_type& key, const data_type& data)
    {
        return insert_start(key, data);
    }

    /// Attempt to insert a key/data pair into the B+ tree. This function is the
    /// same as the other insert, however if key_type == data_type then the
    /// non-template function cannot be called. If the tree does not allow
    /// duplicate keys, then the insert may fail if it is already present.
    inline std::pair<iterator, bool> insert2(const key_type& key, const data_type& data)
    {
        return insert_start(key, data);
    }

    /// Attempt to insert a key/data pair into the B+ tree. The iterator hint
    /// is currently ignored by the B+ tree insertion routine.
    inline iterator insert(iterator /* hint */, const pair_type &x)
    {
        return insert_start(x.first, x.second).first;
    }

    /// Attempt to insert a key/data pair into the B+ tree. The iterator hint is
    /// currently ignored by the B+ tree insertion routine.
    inline iterator insert2(iterator /* hint */, const key_type& key, const data_type& data)
    {
        return insert_start(key, data).first;
    }

    /// Attempt to insert the range [first,last) of value_type pairs into the B+
    /// tree. Each key/data pair is inserted individually.
    template <typename InputIterator>
    inline void insert(InputIterator first, InputIterator last)
    {
        InputIterator iter = first;
        while(iter != last)
        {
            insert(*iter);
            ++iter;
        }
    }

private:
    // *** Private Insertion Functions

    /// Start the insertion descent at the current root and handle root
    /// splits. Returns true if the item was inserted
    std::pair<iterator, bool> insert_start(const key_type& key, const data_type& value)
    {
        node *newchild = NULL;
        key_type newkey = key_type();

        if (root == NULL) {
            root = headleaf = tailleaf = allocate_leaf();
        }

        std::pair<iterator, bool> r = insert_descend(root, key, value, &newkey, &newchild);

        if (newchild)
        {
            inner_node *newroot = allocate_inner(root->level + 1);
            newroot->slotkey[0] = newkey;

            newroot->childid[0] = root;
            newroot->childid[1] = newchild;

            newroot->slotuse = 1;

            root = newroot;
        }

        // increment itemcount if the item was inserted
        if (r.second) ++stats.itemcount;

#ifdef BTREE_DEBUG
        if (debug) print(std::cout);
#endif

        if (selfverify) {
            verify();
            BTREE_ASSERT(exists(key));
        }

        return r;
    }

    /**
     * @brief Insert an item into the B+ tree.
     *
     * Descend down the nodes to a leaf, insert the key/data pair in a free
     * slot. If the node overflows, then it must be split and the new split
     * node inserted into the parent. Unroll / this splitting up to the root.
    */
    std::pair<iterator, bool> insert_descend(node* n,
                                             const key_type& key, const data_type& value,
                                             key_type* splitkey, node** splitnode)
    {
        if (!n->isleafnode())
        {
            inner_node *inner = static_cast<inner_node*>(n);

            key_type newkey = key_type();
            node *newchild = NULL;

            int slot = find_lower(inner, key);

            BTREE_PRINT("btree::insert_descend into " << inner->childid[slot] << std::endl);

            std::pair<iterator, bool> r = insert_descend(inner->childid[slot],
                                                         key, value, &newkey, &newchild);

            if (newchild)
            {
                BTREE_PRINT("btree::insert_descend newchild with key " << newkey << " node " << newchild << " at slot " << slot << std::endl);

                if (inner->isfull())
                {
                    split_inner_node(inner, splitkey, splitnode, slot);

                    BTREE_PRINT("btree::insert_descend done split_inner: putslot: " << slot << " putkey: " << newkey << " upkey: " << *splitkey << std::endl);

#ifdef BTREE_DEBUG
                    if (debug)
                    {
                        print_node(std::cout, inner);
                        print_node(std::cout, *splitnode);
                    }
#endif

                    // check if insert slot is in the split sibling node
                    BTREE_PRINT("btree::insert_descend switch: " << slot << " > " << inner->slotuse+1 << std::endl);

                    if (slot == inner->slotuse+1 && inner->slotuse < (*splitnode)->slotuse)
                    {
                        // special case when the insert slot matches the split
                        // place between the two nodes, then the insert key
                        // becomes the split key.

                        BTREE_ASSERT(inner->slotuse + 1 < innerslotmax);

                        inner_node *splitinner = static_cast<inner_node*>(*splitnode);

                        // move the split key and it's datum into the left node
                        inner->slotkey[inner->slotuse] = *splitkey;
                        inner->childid[inner->slotuse+1] = splitinner->childid[0];
                        inner->slotuse++;

                        // set new split key and move corresponding datum into right node
                        splitinner->childid[0] = newchild;
                        *splitkey = newkey;

                        return r;
                    }
                    else if (slot >= inner->slotuse+1)
                    {
                        // in case the insert slot is in the newly create split
                        // node, we reuse the code below.

                        slot -= inner->slotuse+1;
                        inner = static_cast<inner_node*>(*splitnode);
                        BTREE_PRINT("btree::insert_descend switching to splitted node " << inner << " slot " << slot <<std::endl);
                    }
                }

                // put pointer to child node into correct slot
                BTREE_ASSERT(slot >= 0 && slot <= inner->slotuse);

                int i = inner->slotuse;

                while(i > slot) {
                    inner->slotkey[i] = inner->slotkey[i - 1];
                    inner->childid[i + 1] = inner->childid[i];
                    i--;
                }

                inner->slotkey[slot] = newkey;
                inner->childid[slot + 1] = newchild;
                inner->slotuse++;
            }

            return r;
        }
        else // n->isleafnode() == true
        {
            leaf_node *leaf = static_cast<leaf_node*>(n);

            int slot = find_lower(leaf, key);

            if (!allow_duplicates && slot < leaf->slotuse && key_equal(key, leaf->slotkey[slot])) {
                return std::pair<iterator, bool>(iterator(leaf, slot), false);
            }

            if (leaf->isfull())
            {
                split_leaf_node(leaf, splitkey, splitnode);

                // check if insert slot is in the split sibling node
                if (slot >= leaf->slotuse)
                {
                    slot -= leaf->slotuse;
                    leaf = static_cast<leaf_node*>(*splitnode);
                }
            }

            // put data item into correct data slot

            int i = leaf->slotuse - 1;
            BTREE_ASSERT(i + 1 < leafslotmax);

            while(i >= 0 && key_less(key, leaf->slotkey[i])) {
                leaf->slotkey[i + 1] = leaf->slotkey[i];
                leaf->slotdata[i + 1] = leaf->slotdata[i];
                i--;
            }

            leaf->slotkey[i + 1] = key;
            leaf->slotdata[i + 1] = value;
            leaf->slotuse++;

            if (splitnode && leaf != *splitnode && slot == leaf->slotuse-1)
            {
                // special case: the node was split, and the insert is at the
                // last slot of the old node. then the splitkey must be
                // updated.
                *splitkey = key;
            }

            return std::pair<iterator, bool>(iterator(leaf, i + 1), true);
        }
    }

    /// Split up a leaf node into two equally-filled sibling leaves. Returns
    /// the new nodes and it's insertion key in the two parameters.
    void split_leaf_node(leaf_node* leaf, key_type* _newkey, node** _newleaf)
    {
        BTREE_ASSERT(leaf->isfull());

        unsigned int mid = (leaf->slotuse >> 1);

        BTREE_PRINT("btree::split_leaf_node on " << leaf << std::endl);

        leaf_node *newleaf = allocate_leaf();

        newleaf->slotuse = leaf->slotuse - mid;

        newleaf->nextleaf = leaf->nextleaf;
        if (newleaf->nextleaf == NULL) {
            BTREE_ASSERT(leaf == tailleaf);
            tailleaf = newleaf;
        }
        else {
            newleaf->nextleaf->prevleaf = newleaf;
        }

        for(unsigned int slot = mid; slot < leaf->slotuse; ++slot)
        {
            unsigned int ni = slot - mid;
            newleaf->slotkey[ni] = leaf->slotkey[slot];
            newleaf->slotdata[ni] = leaf->slotdata[slot];
        }

        leaf->slotuse = mid;
        leaf->nextleaf = newleaf;
        newleaf->prevleaf = leaf;

        *_newkey = leaf->slotkey[leaf->slotuse-1];
        *_newleaf = newleaf;
    }

    /// Split up an inner node into two equally-filled sibling nodes. Returns
    /// the new nodes and it's insertion key in the two parameters. Requires
    /// the slot of the item will be inserted, so the nodes will be the same
    /// size after the insert.
    void split_inner_node(inner_node* inner, key_type* _newkey, node** _newinner, unsigned int addslot)
    {
        BTREE_ASSERT(inner->isfull());

        unsigned int mid = (inner->slotuse >> 1);

        BTREE_PRINT("btree::split_inner: mid " << mid << " addslot " << addslot << std::endl);

        // if the split is uneven and the overflowing item will be put into the
        // larger node, then the smaller split node may underflow
        if (addslot <= mid && mid > inner->slotuse - (mid + 1))
            mid--;

        BTREE_PRINT("btree::split_inner: mid " << mid << " addslot " << addslot << std::endl);

        BTREE_PRINT("btree::split_inner_node on " << inner << " into two nodes " << mid << " and " << inner->slotuse - (mid + 1) << " sized" << std::endl);

        inner_node *newinner = allocate_inner(inner->level);

        newinner->slotuse = inner->slotuse - (mid + 1);

        for(unsigned int slot = mid + 1; slot < inner->slotuse; ++slot)
        {
            unsigned int ni = slot - (mid + 1);
            newinner->slotkey[ni] = inner->slotkey[slot];
            newinner->childid[ni] = inner->childid[slot];
        }
        newinner->childid[newinner->slotuse] = inner->childid[inner->slotuse];

        inner->slotuse = mid;

        *_newkey = inner->slotkey[mid];
        *_newinner = newinner;
    }

private:
    // *** Support Class Encapsulating Deletion Results

    /// Result flags of recursive deletion.
    enum result_flags_t
    {
        /// Deletion successful and no fix-ups necessary.
        btree_ok = 0,

        /// Deletion not successful because key was not found.
        btree_not_found = 1,

        /// Deletion successful, the last key was updated so parent slotkeys
        /// need updates.
        btree_update_lastkey = 2,

        /// Deletion successful, children nodes were merged and the parent
        /// needs to remove the empty node.
        btree_fixmerge = 4
    };

    /// B+ tree recursive deletion has much information which is needs to be
    /// passed upward.
    struct result_t
    {
        /// Merged result flags
        result_flags_t  flags;

        /// The key to be updated at the parent's slot
        key_type        lastkey;

        /// Constructor of a result with a specific flag, this can also be used
        /// as for implicit conversion.
        inline result_t(result_flags_t f = btree_ok)
            : flags(f), lastkey()
        { }

        /// Constructor with a lastkey value.
        inline result_t(result_flags_t f, const key_type &k)
            : flags(f), lastkey(k)
        { }

        /// Test if this result object has a given flag set.
        inline bool has(result_flags_t f) const
        {
            return (flags & f) != 0;
        }

        /// Merge two results OR-ing the result flags and overwriting lastkeys.
        inline result_t& operator|= (const result_t &other)
        {
            flags = result_flags_t(flags | other.flags);

            // we overwrite existing lastkeys on purpose
            if (other.has(btree_update_lastkey))
                lastkey = other.lastkey;

            return *this;
        }
    };

public:
    // *** Public Erase Functions

    /// Erases one (the first) of the key/data pairs associated with the given
    /// key.
    bool erase_one(const key_type &key)
    {
        BTREE_PRINT("btree::erase_one(" << key << ") on btree size " << size() << std::endl);

        if (selfverify) verify();

        if (!root) return false;

        result_t result = erase_one_descend(key, root, NULL, NULL, NULL, NULL, NULL, 0);

        if (!result.has(btree_not_found))
            --stats.itemcount;

#ifdef BTREE_DEBUG
        if (debug) print(std::cout);
#endif
        if (selfverify) verify();

        return !result.has(btree_not_found);
    }

    /// Erases all the key/data pairs associated with the given key. This is
    /// implemented using erase_one().
    size_type erase(const key_type &key)
    {
        size_type c = 0;

        while( erase_one(key) )
        {
            ++c;
            if (!allow_duplicates) break;
        }

        return c;
    }

    /// Erase the key/data pair referenced by the iterator.
    void erase(iterator iter)
    {
        BTREE_PRINT("btree::erase_iter(" << iter.currnode << "," << iter.currslot << ") on btree size " << size() << std::endl);

        if (selfverify) verify();

        if (!root) return;

        result_t result = erase_iter_descend(iter, root, NULL, NULL, NULL, NULL, NULL, 0);

        if (!result.has(btree_not_found))
            --stats.itemcount;

#ifdef BTREE_DEBUG
        if (debug) print(std::cout);
#endif
        if (selfverify) verify();
    }

#ifdef BTREE_TODO
    /// Erase all key/data pairs in the range [first,last). This function is
    /// currently not implemented by the B+ Tree.
    void erase(iterator /* first */, iterator /* last */)
    {
        abort();
    }
#endif

private:
    // *** Private Erase Functions

    /** @brief Erase one (the first) key/data pair in the B+ tree matching key.
     *
     * Descends down the tree in search of key. During the descent the parent,
     * left and right siblings and their parents are computed and passed
     * down. Once the key/data pair is found, it is removed from the leaf. If
     * the leaf underflows 6 different cases are handled. These cases resolve
     * the underflow by shifting key/data pairs from adjacent sibling nodes,
     * merging two sibling nodes or trimming the tree.
     */
    result_t erase_one_descend(const key_type& key,
                               node *curr,
                               node *left, node *right,
                               inner_node *leftparent, inner_node *rightparent,
                               inner_node *parent, unsigned int parentslot)
    {
        if (curr->isleafnode())
        {
            leaf_node *leaf = static_cast<leaf_node*>(curr);
            leaf_node *leftleaf = static_cast<leaf_node*>(left);
            leaf_node *rightleaf = static_cast<leaf_node*>(right);

            int slot = find_lower(leaf, key);

            if (slot >= leaf->slotuse || !key_equal(key, leaf->slotkey[slot]))
            {
                BTREE_PRINT("Could not find key " << key << " to erase." << std::endl);

                return btree_not_found;
            }

            BTREE_PRINT("Found key in leaf " << curr << " at slot " << slot << std::endl);

            for (int i = slot; i < leaf->slotuse - 1; i++)
            {
                leaf->slotkey[i] = leaf->slotkey[i + 1];
                leaf->slotdata[i] = leaf->slotdata[i + 1];
            }
            leaf->slotuse--;

            result_t myres = btree_ok;

            // if the last key of the leaf was changed, the parent is notified
            // and updates the key of this leaf
            if (slot == leaf->slotuse)
            {
                if (parent && parentslot < parent->slotuse)
                {
                    BTREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = leaf->slotkey[leaf->slotuse - 1];
                }
                else
                {
                    if (leaf->slotuse >= 1)
                    {
                        BTREE_PRINT("Scheduling lastkeyupdate: key " << leaf->slotkey[leaf->slotuse - 1] << std::endl);
                        myres |= result_t(btree_update_lastkey, leaf->slotkey[leaf->slotuse - 1]);
                    }
                    else
                    {
                        BTREE_ASSERT(leaf == root);
                    }
                }
            }

            if (leaf->isunderflow() && !(leaf == root && leaf->slotuse >= 1))
            {
                // determine what to do about the underflow

                // case : if this empty leaf is the root, then delete all nodes
                // and set root to NULL.
                if (leftleaf == NULL && rightleaf == NULL)
                {
                    BTREE_ASSERT(leaf == root);
                    BTREE_ASSERT(leaf->slotuse == 0);

                    free_node(root);

                    root = leaf = NULL;
                    headleaf = tailleaf = NULL;

                    // will be decremented soon by insert_start()
                    BTREE_ASSERT(stats.itemcount == 1);
                    BTREE_ASSERT(stats.leaves == 0);
                    BTREE_ASSERT(stats.innernodes == 0);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case of
                // a shift, then merging is necessary. choose the more local merger
                // with our parent
                else if ( (leftleaf == NULL || leftleaf->isfew()) && (rightleaf == NULL || rightleaf->isfew()) )
                {
                    if (leftparent == parent)
                        myres |= merge_leaves(leftleaf, leaf, leftparent);
                    else
                        myres |= merge_leaves(leaf, rightleaf, rightparent);
                }
                // case : the right leaf has extra data, so balance right with current
                else if ( (leftleaf != NULL && leftleaf->isfew()) && (rightleaf != NULL && !rightleaf->isfew()) )
                {
                    if (rightparent == parent)
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                    else
                        myres |= merge_leaves(leftleaf, leaf, leftparent);
                }
                // case : the left leaf has extra data, so balance left with current
                else if ( (leftleaf != NULL && !leftleaf->isfew()) && (rightleaf != NULL && rightleaf->isfew()) )
                {
                    if (leftparent == parent)
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                    else
                        myres |= merge_leaves(leaf, rightleaf, rightparent);
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (leftparent == rightparent)
                {
                    if (leftleaf->slotuse <= rightleaf->slotuse)
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                    else
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                }
                else
                {
                    if (leftparent == parent)
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                    else
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                }
            }

            return myres;
        }
        else // !curr->isleafnode()
        {
            inner_node *inner = static_cast<inner_node*>(curr);
            inner_node *leftinner = static_cast<inner_node*>(left);
            inner_node *rightinner = static_cast<inner_node*>(right);

            node *myleft, *myright;
            inner_node *myleftparent, *myrightparent;

            int slot = find_lower(inner, key);

            if (slot == 0) {
                myleft = (left == NULL) ? NULL : (static_cast<inner_node*>(left))->childid[left->slotuse - 1];
                myleftparent = leftparent;
            }
            else {
                myleft = inner->childid[slot - 1];
                myleftparent = inner;
            }

            if (slot == inner->slotuse) {
                myright = (right == NULL) ? NULL : (static_cast<inner_node*>(right))->childid[0];
                myrightparent = rightparent;
            }
            else {
                myright = inner->childid[slot + 1];
                myrightparent = inner;
            }

            BTREE_PRINT("erase_one_descend into " << inner->childid[slot] << std::endl);

            result_t result = erase_one_descend(key,
                                                inner->childid[slot],
                                                myleft, myright,
                                                myleftparent, myrightparent,
                                                inner, slot);

            result_t myres = btree_ok;

            if (result.has(btree_not_found))
            {
                return result;
            }

            if (result.has(btree_update_lastkey))
            {
                if (parent && parentslot < parent->slotuse)
                {
                    BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent << " at parentslot " << parentslot << std::endl);

                    BTREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = result.lastkey;
                }
                else
                {
                    BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
                    myres |= result_t(btree_update_lastkey, result.lastkey);
                }
            }

            if (result.has(btree_fixmerge))
            {
                // either the current node or the next is empty and should be removed
                if (inner->childid[slot]->slotuse != 0)
                    slot++;

                // this is the child slot invalidated by the merge
                BTREE_ASSERT(inner->childid[slot]->slotuse == 0);

                free_node(inner->childid[slot]);

                for(int i = slot; i < inner->slotuse; i++)
                {
                    inner->slotkey[i - 1] = inner->slotkey[i];
                    inner->childid[i] = inner->childid[i + 1];
                }
                inner->slotuse--;

                if (inner->level == 1)
                {
                    // fix split key for children leaves
                    slot--;
                    leaf_node *child = static_cast<leaf_node*>(inner->childid[slot]);
                    inner->slotkey[slot] = child->slotkey[ child->slotuse-1 ];
                }
            }

            if (inner->isunderflow() && !(inner == root && inner->slotuse >= 1))
            {
                // case: the inner node is the root and has just one child. that child becomes the new root
                if (leftinner == NULL && rightinner == NULL)
                {
                    BTREE_ASSERT(inner == root);
                    BTREE_ASSERT(inner->slotuse == 0);

                    root = inner->childid[0];

                    inner->slotuse = 0;
                    free_node(inner);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case of
                // a shift, then merging is necessary. choose the more local merger
                // with our parent
                else if ( (leftinner == NULL || leftinner->isfew()) && (rightinner == NULL || rightinner->isfew()) )
                {
                    if (leftparent == parent)
                        myres |= merge_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        myres |= merge_inner(inner, rightinner, rightparent, parentslot);
                }
                // case : the right leaf has extra data, so balance right with current
                else if ( (leftinner != NULL && leftinner->isfew()) && (rightinner != NULL && !rightinner->isfew()) )
                {
                    if (rightparent == parent)
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                    else
                        myres |= merge_inner(leftinner, inner, leftparent, parentslot - 1);
                }
                // case : the left leaf has extra data, so balance left with current
                else if ( (leftinner != NULL && !leftinner->isfew()) && (rightinner != NULL && rightinner->isfew()) )
                {
                    if (leftparent == parent)
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        myres |= merge_inner(inner, rightinner, rightparent, parentslot);
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (leftparent == rightparent)
                {
                    if (leftinner->slotuse <= rightinner->slotuse)
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                    else
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                }
                else
                {
                    if (leftparent == parent)
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                }
            }

            return myres;
        }
    }

    /** @brief Erase one key/data pair referenced by an iterator in the B+
     * tree.
     *
     * Descends down the tree in search of an iterator. During the descent the
     * parent, left and right siblings and their parents are computed and
     * passed down. The difficulty is that the iterator contains only a pointer
     * to a leaf_node, which means that this function must do a recursive depth
     * first search for that leaf node in the subtree containing all pairs of
     * the same key. This subtree can be very large, even the whole tree,
     * though in practice it would not make sense to have so many duplicate
     * keys.
     *
     * Once the referenced key/data pair is found, it is removed from the leaf
     * and the same underflow cases are handled as in erase_one_descend.
     */
    result_t erase_iter_descend(const iterator& iter,
				node *curr,
				node *left, node *right,
				inner_node *leftparent, inner_node *rightparent,
				inner_node *parent, unsigned int parentslot)
    {
        if (curr->isleafnode())
        {
            leaf_node *leaf = static_cast<leaf_node*>(curr);
            leaf_node *leftleaf = static_cast<leaf_node*>(left);
            leaf_node *rightleaf = static_cast<leaf_node*>(right);

	    // if this is not the correct leaf, get next step in recursive
	    // search
	    if (leaf != iter.currnode)
	    {
		return btree_not_found;
	    }

            if (iter.currslot >= leaf->slotuse)
            {
                BTREE_PRINT("Could not find iterator (" << iter.currnode << "," << iter.currslot << ") to erase. Invalid leaf node?" << std::endl);

                return btree_not_found;
            }

	    int slot = iter.currslot;

            BTREE_PRINT("Found iterator in leaf " << curr << " at slot " << slot << std::endl);

            for (int i = slot; i < leaf->slotuse - 1; i++)
            {
                leaf->slotkey[i] = leaf->slotkey[i + 1];
                leaf->slotdata[i] = leaf->slotdata[i + 1];
            }
            leaf->slotuse--;

            result_t myres = btree_ok;

            // if the last key of the leaf was changed, the parent is notified
            // and updates the key of this leaf
            if (slot == leaf->slotuse)
            {
                if (parent && parentslot < parent->slotuse)
                {
                    BTREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = leaf->slotkey[leaf->slotuse - 1];
                }
                else
                {
                    if (leaf->slotuse >= 1)
                    {
                        BTREE_PRINT("Scheduling lastkeyupdate: key " << leaf->slotkey[leaf->slotuse - 1] << std::endl);
                        myres |= result_t(btree_update_lastkey, leaf->slotkey[leaf->slotuse - 1]);
                    }
                    else
                    {
                        BTREE_ASSERT(leaf == root);
                    }
                }
            }

            if (leaf->isunderflow() && !(leaf == root && leaf->slotuse >= 1))
            {
                // determine what to do about the underflow

                // case : if this empty leaf is the root, then delete all nodes
                // and set root to NULL.
                if (leftleaf == NULL && rightleaf == NULL)
                {
                    BTREE_ASSERT(leaf == root);
                    BTREE_ASSERT(leaf->slotuse == 0);

                    free_node(root);

                    root = leaf = NULL;
                    headleaf = tailleaf = NULL;

                    // will be decremented soon by insert_start()
                    BTREE_ASSERT(stats.itemcount == 1);
                    BTREE_ASSERT(stats.leaves == 0);
                    BTREE_ASSERT(stats.innernodes == 0);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case of
                // a shift, then merging is necessary. choose the more local merger
                // with our parent
                else if ( (leftleaf == NULL || leftleaf->isfew()) && (rightleaf == NULL || rightleaf->isfew()) )
                {
                    if (leftparent == parent)
                        myres |= merge_leaves(leftleaf, leaf, leftparent);
                    else
                        myres |= merge_leaves(leaf, rightleaf, rightparent);
                }
                // case : the right leaf has extra data, so balance right with current
                else if ( (leftleaf != NULL && leftleaf->isfew()) && (rightleaf != NULL && !rightleaf->isfew()) )
                {
                    if (rightparent == parent)
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                    else
                        myres |= merge_leaves(leftleaf, leaf, leftparent);
                }
                // case : the left leaf has extra data, so balance left with current
                else if ( (leftleaf != NULL && !leftleaf->isfew()) && (rightleaf != NULL && rightleaf->isfew()) )
                {
                    if (leftparent == parent)
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                    else
                        myres |= merge_leaves(leaf, rightleaf, rightparent);
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (leftparent == rightparent)
                {
                    if (leftleaf->slotuse <= rightleaf->slotuse)
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                    else
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                }
                else
                {
                    if (leftparent == parent)
                        shift_right_leaf(leftleaf, leaf, leftparent, parentslot - 1);
                    else
                        myres |= shift_left_leaf(leaf, rightleaf, rightparent, parentslot);
                }
            }

            return myres;
        }
        else // !curr->isleafnode()
        {
            inner_node *inner = static_cast<inner_node*>(curr);
            inner_node *leftinner = static_cast<inner_node*>(left);
            inner_node *rightinner = static_cast<inner_node*>(right);

	    // find first slot below which the searched iterator might be
	    // located.

	    result_t result;
            int slot = find_lower(inner, iter.key());

	    while (slot <= inner->slotuse)
	    {
		node *myleft, *myright;
		inner_node *myleftparent, *myrightparent;

		if (slot == 0) {
		    myleft = (left == NULL) ? NULL : (static_cast<inner_node*>(left))->childid[left->slotuse - 1];
		    myleftparent = leftparent;
		}
		else {
		    myleft = inner->childid[slot - 1];
		    myleftparent = inner;
		}

		if (slot == inner->slotuse) {
		    myright = (right == NULL) ? NULL : (static_cast<inner_node*>(right))->childid[0];
		    myrightparent = rightparent;
		}
		else {
		    myright = inner->childid[slot + 1];
		    myrightparent = inner;
		}

		BTREE_PRINT("erase_iter_descend into " << inner->childid[slot] << std::endl);

		result = erase_iter_descend(iter,
					    inner->childid[slot],
					    myleft, myright,
					    myleftparent, myrightparent,
					    inner, slot);

		if (!result.has(btree_not_found))
		    break;

		// continue recursive search for leaf on next slot

		if (slot < inner->slotuse && key_less(inner->slotkey[slot],iter.key()))
		    return btree_not_found;

		++slot;
	    }

	    if (slot > inner->slotuse)
		return btree_not_found;

	    result_t myres = btree_ok;

            if (result.has(btree_update_lastkey))
            {
                if (parent && parentslot < parent->slotuse)
                {
                    BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent << " at parentslot " << parentslot << std::endl);

                    BTREE_ASSERT(parent->childid[parentslot] == curr);
                    parent->slotkey[parentslot] = result.lastkey;
                }
                else
                {
                    BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
                    myres |= result_t(btree_update_lastkey, result.lastkey);
                }
            }

            if (result.has(btree_fixmerge))
            {
                // either the current node or the next is empty and should be removed
                if (inner->childid[slot]->slotuse != 0)
                    slot++;

                // this is the child slot invalidated by the merge
                BTREE_ASSERT(inner->childid[slot]->slotuse == 0);

                free_node(inner->childid[slot]);

                for(int i = slot; i < inner->slotuse; i++)
                {
                    inner->slotkey[i - 1] = inner->slotkey[i];
                    inner->childid[i] = inner->childid[i + 1];
                }
                inner->slotuse--;

                if (inner->level == 1)
                {
                    // fix split key for children leaves
                    slot--;
                    leaf_node *child = static_cast<leaf_node*>(inner->childid[slot]);
                    inner->slotkey[slot] = child->slotkey[ child->slotuse-1 ];
                }
            }

            if (inner->isunderflow() && !(inner == root && inner->slotuse >= 1))
            {
                // case: the inner node is the root and has just one child. that child becomes the new root
                if (leftinner == NULL && rightinner == NULL)
                {
                    BTREE_ASSERT(inner == root);
                    BTREE_ASSERT(inner->slotuse == 0);

                    root = inner->childid[0];

                    inner->slotuse = 0;
                    free_node(inner);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case of
                // a shift, then merging is necessary. choose the more local merger
                // with our parent
                else if ( (leftinner == NULL || leftinner->isfew()) && (rightinner == NULL || rightinner->isfew()) )
                {
                    if (leftparent == parent)
                        myres |= merge_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        myres |= merge_inner(inner, rightinner, rightparent, parentslot);
                }
                // case : the right leaf has extra data, so balance right with current
                else if ( (leftinner != NULL && leftinner->isfew()) && (rightinner != NULL && !rightinner->isfew()) )
                {
                    if (rightparent == parent)
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                    else
                        myres |= merge_inner(leftinner, inner, leftparent, parentslot - 1);
                }
                // case : the left leaf has extra data, so balance left with current
                else if ( (leftinner != NULL && !leftinner->isfew()) && (rightinner != NULL && rightinner->isfew()) )
                {
                    if (leftparent == parent)
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        myres |= merge_inner(inner, rightinner, rightparent, parentslot);
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (leftparent == rightparent)
                {
                    if (leftinner->slotuse <= rightinner->slotuse)
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                    else
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                }
                else
                {
                    if (leftparent == parent)
                        shift_right_inner(leftinner, inner, leftparent, parentslot - 1);
                    else
                        shift_left_inner(inner, rightinner, rightparent, parentslot);
                }
            }

            return myres;
        }
    }

    /// Merge two leaf nodes. The function moves all key/data pairs from right
    /// to left and sets right's slotuse to zero. The right slot is then
    /// removed by the calling parent node.
    result_t merge_leaves(leaf_node* left, leaf_node* right, inner_node* parent)
    {
        BTREE_PRINT("Merge leaf nodes " << left << " and " << right << " with common parent " << parent << "." << std::endl);
        (void)parent;

        BTREE_ASSERT(left->isleafnode() && right->isleafnode());
        BTREE_ASSERT(parent->level == 1);

        BTREE_ASSERT(left->slotuse + right->slotuse < leafslotmax);

        for (unsigned int i = 0; i < right->slotuse; i++)
        {
            left->slotkey[left->slotuse + i] = right->slotkey[i];
            left->slotdata[left->slotuse + i] = right->slotdata[i];
        }
        left->slotuse += right->slotuse;

        left->nextleaf = right->nextleaf;
        if (left->nextleaf)
            left->nextleaf->prevleaf = left;
        else
            tailleaf = left;

        right->slotuse = 0;

        return btree_fixmerge;
    }

    /// Merge two inner nodes. The function moves all key/childid pairs from
    /// right to left and sets right's slotuse to zero. The right slot is then
    /// removed by the calling parent node.
    static result_t merge_inner(inner_node* left, inner_node* right, inner_node* parent, unsigned int parentslot)
    {
        BTREE_PRINT("Merge inner nodes " << left << " and " << right << " with common parent " << parent << "." << std::endl);

        BTREE_ASSERT(left->level == right->level);
        BTREE_ASSERT(parent->level == left->level + 1);

        BTREE_ASSERT(parent->childid[parentslot] == left);

        BTREE_ASSERT(left->slotuse + right->slotuse < innerslotmax);

        if (selfverify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while(leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            BTREE_ASSERT(leftslot < parent->slotuse);
            BTREE_ASSERT(parent->childid[leftslot] == left);
            BTREE_ASSERT(parent->childid[leftslot+1] == right);

            BTREE_ASSERT(parentslot == leftslot);
        }

        // retrieve the decision key from parent
        left->slotkey[left->slotuse] = parent->slotkey[parentslot];
        left->slotuse++;

        // copy over keys and children from right
        for (unsigned int i = 0; i < right->slotuse; i++)
        {
            left->slotkey[left->slotuse + i] = right->slotkey[i];
            left->childid[left->slotuse + i] = right->childid[i];
        }
        left->slotuse += right->slotuse;

        left->childid[left->slotuse] = right->childid[right->slotuse];

        right->slotuse = 0;

        return btree_fixmerge;
    }

    /// Balance two leaf nodes. The function moves key/data pairs from right to
    /// left so that both nodes are equally filled. The parent node is updated
    /// if possible.
    static result_t shift_left_leaf(leaf_node *left, leaf_node *right, inner_node *parent, unsigned int parentslot)
    {
        BTREE_ASSERT(left->isleafnode() && right->isleafnode());
        BTREE_ASSERT(parent->level == 1);

        BTREE_ASSERT(left->nextleaf == right);
        BTREE_ASSERT(left == right->prevleaf);

        BTREE_ASSERT(left->slotuse < right->slotuse);
        BTREE_ASSERT(parent->childid[parentslot] == left);

        unsigned int shiftnum = (right->slotuse - left->slotuse) >> 1;

        BTREE_PRINT("Shifting (leaf) " << shiftnum << " entries to left " << left << " from right " << right << " with common parent " << parent << "." << std::endl);

        BTREE_ASSERT(left->slotuse + shiftnum < leafslotmax);

        // copy the first items from the right node to the last slot in the left node.
        for(unsigned int i = 0; i < shiftnum; i++)
        {
            left->slotkey[left->slotuse + i] = right->slotkey[i];
            left->slotdata[left->slotuse + i] = right->slotdata[i];
        }
        left->slotuse += shiftnum;

        // shift all slots in the right node to the left

        right->slotuse -= shiftnum;
        for(int i = 0; i < right->slotuse; i++)
        {
            right->slotkey[i] = right->slotkey[i + shiftnum];
            right->slotdata[i] = right->slotdata[i + shiftnum];
        }

        // fixup parent
        if (parentslot < parent->slotuse) {
            parent->slotkey[parentslot] = left->slotkey[left->slotuse - 1];
            return btree_ok;
        }
        else { // the update is further up the tree
            return result_t(btree_update_lastkey, left->slotkey[left->slotuse - 1]);
        }
    }

    /// Balance two inner nodes. The function moves key/data pairs from right
    /// to left so that both nodes are equally filled. The parent node is
    /// updated if possible.
    static void shift_left_inner(inner_node *left, inner_node *right, inner_node *parent, unsigned int parentslot)
    {
        BTREE_ASSERT(left->level == right->level);
        BTREE_ASSERT(parent->level == left->level + 1);

        BTREE_ASSERT(left->slotuse < right->slotuse);
        BTREE_ASSERT(parent->childid[parentslot] == left);

        unsigned int shiftnum = (right->slotuse - left->slotuse) >> 1;

        BTREE_PRINT("Shifting (inner) " << shiftnum << " entries to left " << left << " from right " << right << " with common parent " << parent << "." << std::endl);

        BTREE_ASSERT(left->slotuse + shiftnum < innerslotmax);

        if (selfverify)
        {
            // find the left node's slot in the parent's children and compare to parentslot

            unsigned int leftslot = 0;
            while(leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            BTREE_ASSERT(leftslot < parent->slotuse);
            BTREE_ASSERT(parent->childid[leftslot] == left);
            BTREE_ASSERT(parent->childid[leftslot+1] == right);

            BTREE_ASSERT(leftslot == parentslot);
        }

        // copy the parent's decision slotkey and childid to the first new key on the left
        left->slotkey[left->slotuse] = parent->slotkey[parentslot];
        left->slotuse++;

        // copy the other items from the right node to the last slots in the left node.
        for(unsigned int i = 0; i < shiftnum - 1; i++)
        {
            left->slotkey[left->slotuse + i] = right->slotkey[i];
            left->childid[left->slotuse + i] = right->childid[i];
        }
        left->slotuse += shiftnum - 1;

        // fixup parent
        parent->slotkey[parentslot] = right->slotkey[shiftnum - 1];
        // last pointer in left
        left->childid[left->slotuse] = right->childid[shiftnum - 1];

        // shift all slots in the right node

        right->slotuse -= shiftnum;
        for(int i = 0; i < right->slotuse; i++)
        {
            right->slotkey[i] = right->slotkey[i + shiftnum];
            right->childid[i] = right->childid[i + shiftnum];
        }
        right->childid[right->slotuse] = right->childid[right->slotuse + shiftnum];
    }

    /// Balance two leaf nodes. The function moves key/data pairs from left to
    /// right so that both nodes are equally filled. The parent node is updated
    /// if possible.
    static void shift_right_leaf(leaf_node *left, leaf_node *right, inner_node *parent, unsigned int parentslot)
    {
        BTREE_ASSERT(left->isleafnode() && right->isleafnode());
        BTREE_ASSERT(parent->level == 1);

        BTREE_ASSERT(left->nextleaf == right);
        BTREE_ASSERT(left == right->prevleaf);
        BTREE_ASSERT(parent->childid[parentslot] == left);

        BTREE_ASSERT(left->slotuse > right->slotuse);

        unsigned int shiftnum = (left->slotuse - right->slotuse) >> 1;

        BTREE_PRINT("Shifting (leaf) " << shiftnum << " entries to right " << right << " from left " << left << " with common parent " << parent << "." << std::endl);

        if (selfverify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while(leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            BTREE_ASSERT(leftslot < parent->slotuse);
            BTREE_ASSERT(parent->childid[leftslot] == left);
            BTREE_ASSERT(parent->childid[leftslot+1] == right);

            BTREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        BTREE_ASSERT(right->slotuse + shiftnum < leafslotmax);

        for(int i = right->slotuse-1; i >= 0; i--)
        {
            right->slotkey[i + shiftnum] = right->slotkey[i];
            right->slotdata[i + shiftnum] = right->slotdata[i];
        }
        right->slotuse += shiftnum;

        // copy the last items from the left node to the first slot in the right node.
        for(unsigned int i = 0; i < shiftnum; i++)
        {
            right->slotkey[i] = left->slotkey[left->slotuse - shiftnum + i];
            right->slotdata[i] = left->slotdata[left->slotuse - shiftnum + i];
        }
        left->slotuse -= shiftnum;

        parent->slotkey[parentslot] = left->slotkey[left->slotuse-1];
    }

    /// Balance two inner nodes. The function moves key/data pairs from left to
    /// right so that both nodes are equally filled. The parent node is updated
    /// if possible.
    static void shift_right_inner(inner_node *left, inner_node *right, inner_node *parent, unsigned int parentslot)
    {
        BTREE_ASSERT(left->level == right->level);
        BTREE_ASSERT(parent->level == left->level + 1);

        BTREE_ASSERT(left->slotuse > right->slotuse);
        BTREE_ASSERT(parent->childid[parentslot] == left);

        unsigned int shiftnum = (left->slotuse - right->slotuse) >> 1;

        BTREE_PRINT("Shifting (leaf) " << shiftnum << " entries to right " << right << " from left " << left << " with common parent " << parent << "." << std::endl);

        if (selfverify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while(leftslot <= parent->slotuse && parent->childid[leftslot] != left)
                ++leftslot;

            BTREE_ASSERT(leftslot < parent->slotuse);
            BTREE_ASSERT(parent->childid[leftslot] == left);
            BTREE_ASSERT(parent->childid[leftslot+1] == right);

            BTREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        BTREE_ASSERT(right->slotuse + shiftnum < innerslotmax);

        right->childid[right->slotuse + shiftnum] = right->childid[right->slotuse];
        for(int i = right->slotuse-1; i >= 0; i--)
        {
            right->slotkey[i + shiftnum] = right->slotkey[i];
            right->childid[i + shiftnum] = right->childid[i];
        }
        right->slotuse += shiftnum;

        // copy the parent's decision slotkey and childid to the last new key on the right
        right->slotkey[shiftnum - 1] = parent->slotkey[parentslot];
        right->childid[shiftnum - 1] = left->childid[left->slotuse];

        // copy the remaining last items from the left node to the first slot in the right node.
        for(unsigned int i = 0; i < shiftnum - 1; i++)
        {
            right->slotkey[i] = left->slotkey[left->slotuse - shiftnum + i + 1];
            right->childid[i] = left->childid[left->slotuse - shiftnum + i + 1];
        }

        // copy the first to-be-removed key from the left node to the parent's decision slot
        parent->slotkey[parentslot] = left->slotkey[left->slotuse - shiftnum];

        left->slotuse -= shiftnum;
    }

#ifdef BTREE_DEBUG
public:
    // *** Debug Printing

    /// Print out the B+ tree structure with keys onto the given ostream. This
    /// function requires that the header is compiled with BTREE_DEBUG and that
    /// key_type is printable via std::ostream.
    void print(std::ostream &os) const
    {
        if (root) {
            print_node(os, root, 0, true);
        }
    }

    /// Print out only the leaves via the double linked list.
    void print_leaves(std::ostream &os) const
    {
        os << "leaves:" << std::endl;

        const leaf_node *n = headleaf;

        while(n)
        {
            os << "  " << n << std::endl;

            n = n->nextleaf;
        }
    }

private:

    /// Recursively descend down the tree and print out nodes.
    static void print_node(std::ostream &os, const node* node, unsigned int depth=0, bool recursive=false)
    {
        for(unsigned int i = 0; i < depth; i++) os << "  ";

        os << "node " << node << " level " << node->level << " slotuse " << node->slotuse << std::endl;

        if (node->isleafnode())
        {
            const leaf_node *leafnode = static_cast<const leaf_node*>(node);

            for(unsigned int i = 0; i < depth; i++) os << "  ";
            os << "  leaf prev " << leafnode->prevleaf << " next " << leafnode->nextleaf << std::endl;

            for(unsigned int i = 0; i < depth; i++) os << "  ";

            for (unsigned int slot = 0; slot < leafnode->slotuse; ++slot)
            {
                os << leafnode->slotkey[slot] << "  "; // << "(data: " << leafnode->slotdata[slot] << ") ";
            }
            os << std::endl;
        }
        else
        {
            const inner_node *innernode = static_cast<const inner_node*>(node);

            for(unsigned int i = 0; i < depth; i++) os << "  ";

            for (unsigned short slot = 0; slot < innernode->slotuse; ++slot)
            {
                os << "(" << innernode->childid[slot] << ") " << innernode->slotkey[slot] << " ";
            }
            os << "(" << innernode->childid[innernode->slotuse] << ")" << std::endl;

            if (recursive)
            {
                for (unsigned short slot = 0; slot < innernode->slotuse + 1; ++slot)
                {
                    print_node(os, innernode->childid[slot], depth + 1, recursive);
                }
            }
        }
    }
#endif

public:
    // *** Verification of B+ Tree Invariants

    /// Run a thorough verification of all B+ tree invariants. The program
    /// aborts via assert() if something is wrong.
    void verify() const
    {
        key_type minkey, maxkey;
        tree_stats vstats;

        if (root)
        {
            verify_node(root, &minkey, &maxkey, vstats);

            assert( vstats.itemcount == stats.itemcount );
            assert( vstats.leaves == stats.leaves );
            assert( vstats.innernodes == stats.innernodes );

            verify_leaflinks();
        }
    }

private:

    /// Recursively descend down the tree and verify each node
    void verify_node(const node* n, key_type* minkey, key_type* maxkey, tree_stats &vstats) const
    {
        BTREE_PRINT("verifynode " << n << std::endl);

        if (n->isleafnode())
        {
            const leaf_node *leaf = static_cast<const leaf_node*>(n);

            assert( leaf == root || !leaf->isunderflow() );
            assert( leaf->slotuse > 0 );

            for(unsigned short slot = 0; slot < leaf->slotuse - 1; ++slot)
            {
                assert(key_lessequal(leaf->slotkey[slot], leaf->slotkey[slot + 1]));
            }

            *minkey = leaf->slotkey[0];
            *maxkey = leaf->slotkey[leaf->slotuse - 1];

            vstats.leaves++;
            vstats.itemcount += leaf->slotuse;
        }
        else // !n->isleafnode()
        {
            const inner_node *inner = static_cast<const inner_node*>(n);
            vstats.innernodes++;

            assert( inner == root || !inner->isunderflow() );
            assert( inner->slotuse > 0 );

            for(unsigned short slot = 0; slot < inner->slotuse - 1; ++slot)
            {
                assert(key_lessequal(inner->slotkey[slot], inner->slotkey[slot + 1]));
            }

            for(unsigned short slot = 0; slot <= inner->slotuse; ++slot)
            {
                const node *subnode = inner->childid[slot];
                key_type subminkey = key_type();
                key_type submaxkey = key_type();

                assert(subnode->level + 1 == inner->level);
                verify_node(subnode, &subminkey, &submaxkey, vstats);

                BTREE_PRINT("verify subnode " << subnode << ": " << subminkey << " - " << submaxkey << std::endl);

                if (slot == 0)
                    *minkey = subminkey;
                else
                    assert(key_greaterequal(subminkey, inner->slotkey[slot-1]));

                if (slot == inner->slotuse)
                    *maxkey = submaxkey;
                else
                    assert(key_equal(inner->slotkey[slot], submaxkey));

                if (inner->level == 1 && slot < inner->slotuse)
                {
                    // children are leaves and must be linked together in the
                    // correct order
                    const leaf_node *leafa = static_cast<const leaf_node*>(inner->childid[slot]);
                    const leaf_node *leafb = static_cast<const leaf_node*>(inner->childid[slot + 1]);

                    assert(leafa->nextleaf == leafb);
                    assert(leafa == leafb->prevleaf);
                    (void)leafa; (void)leafb;
                }
                if (inner->level == 2 && slot < inner->slotuse)
                {
                    // verify leaf links between the adjacent inner nodes
                    const inner_node *parenta = static_cast<const inner_node*>(inner->childid[slot]);
                    const inner_node *parentb = static_cast<const inner_node*>(inner->childid[slot+1]);

                    const leaf_node *leafa = static_cast<const leaf_node*>(parenta->childid[parenta->slotuse]);
                    const leaf_node *leafb = static_cast<const leaf_node*>(parentb->childid[0]);

                    assert(leafa->nextleaf == leafb);
                    assert(leafa == leafb->prevleaf);
                    (void)leafa; (void)leafb;
                }
            }
        }
    }

    /// Verify the double linked list of leaves.
    void verify_leaflinks() const
    {
        const leaf_node *n = headleaf;

        assert(n->level == 0);
        assert(!n || n->prevleaf == NULL);

        unsigned int testcount = 0;

        while(n)
        {
            assert(n->level == 0);
            assert(n->slotuse > 0);

            for(unsigned short slot = 0; slot < n->slotuse - 1; ++slot)
            {
                assert(key_lessequal(n->slotkey[slot], n->slotkey[slot + 1]));
            }

            testcount += n->slotuse;

            if (n->nextleaf)
            {
                assert(key_lessequal(n->slotkey[n->slotuse-1], n->nextleaf->slotkey[0]));

                assert(n == n->nextleaf->prevleaf);
            }
            else
            {
                assert(tailleaf == n);
            }

            n = n->nextleaf;
        }

        assert(testcount == size());
    }

private:
    // *** Dump and Restore of B+ Trees

    /// A header for the binary image containing the base properties of the B+
    /// tree. These properties have to match the current template
    /// instantiation.
    struct dump_header
    {
        /// "stx-btree", just to stop the restore() function from loading garbage
        char            signature[12];

        /// Currently 0
        unsigned short  version;

        /// sizeof(key_type)
        unsigned short  key_type_size;

        /// sizeof(data_type)
        unsigned short  data_type_size;

        /// Number of slots in the leaves
        unsigned short  leafslots;

        /// Number of slots in the inner nodes
        unsigned short  innerslots;

        /// Allow duplicates
        bool            allow_duplicates;

        /// The item count of the tree
        size_type       itemcount;

        /// Fill the struct with the current B+ tree's properties, itemcount is
        /// not filled.
        inline void fill()
        {
            // don't want to include string.h just for this signature
	    signature[0] = 's'; signature[1] = 't'; signature[2] = 'x'; signature[3] = '-';
	    signature[4] = 'b'; signature[5] = 't'; signature[6] = 'r'; signature[7] = 'e';
	    signature[8] = 'e'; signature[9] = 0; signature[10] = 0; signature[11] = 0;

            version = 0;
            key_type_size = sizeof(typename btree_self::key_type);
            data_type_size = sizeof(typename btree_self::data_type);
            leafslots = btree_self::leafslotmax;
            innerslots = btree_self::innerslotmax;
            allow_duplicates = btree_self::allow_duplicates;
        }

        /// Returns true if the headers have the same vital properties
        inline bool same(const struct dump_header &o) const
        {
            return (signature[0] == 's' && signature[1] == 't' && signature[2] == 'x' && signature[3] == '-' &&
		    signature[4] == 'b' && signature[5] == 't' && signature[6] == 'r' && signature[7] == 'e' &&
		    signature[8] == 'e' && signature[9] == 0 && signature[10] == 0 && signature[11] == 0)
	        && (version == o.version)
                && (key_type_size == o.key_type_size)
                && (data_type_size == o.data_type_size)
                && (leafslots == o.leafslots)
                && (innerslots == o.innerslots)
                && (allow_duplicates == o.allow_duplicates);
        }
    };

public:

    /// Dump the contents of the B+ tree out onto an ostream as a binary
    /// image. The image contains memory pointers which will be fixed when the
    /// image is restored. For this to work your key_type and data_type must be
    /// integral types and contain no pointers or references.
    void dump(std::ostream &os) const
    {
        struct dump_header header;
        header.fill();
        header.itemcount = size();

        os.write(reinterpret_cast<char*>(&header), sizeof(header));

        if (root) {
            dump_node(os, root);
        }
    }

    /// Restore a binary image of a dumped B+ tree from an istream. The B+ tree
    /// pointers are fixed using the dump order. For dump and restore to work
    /// your key_type and data_type must be integral types and contain no
    /// pointers or references. Returns true if the restore was successful.
    bool restore(std::istream &is)
    {
        struct dump_header fileheader;
        is.read(reinterpret_cast<char*>(&fileheader), sizeof(fileheader));
        if (!is.good()) return false;

        struct dump_header myheader;
        myheader.fill();
        myheader.itemcount = fileheader.itemcount;

        if (!myheader.same(fileheader))
        {
            BTREE_PRINT("btree::restore: file header does not match instantiation signature." << std::endl);
            return false;
        }

        clear();

        if (fileheader.itemcount > 0)
        {
            root = restore_node(is);
            if (root == NULL) return false;

            stats.itemcount = fileheader.itemcount;
        }

#ifdef BTREE_DEBUG
        if (debug) print(std::cout);
#endif
        if (selfverify) verify();

        return true;
    }

private:

    /// Recursively descend down the tree and dump each node in a precise order
    void dump_node(std::ostream &os, const node* n) const
    {
        BTREE_PRINT("dump_node " << n << std::endl);

        if (n->isleafnode())
        {
            const leaf_node *leaf = static_cast<const leaf_node*>(n);

            os.write(reinterpret_cast<const char*>(leaf), sizeof(*leaf));
        }
        else // !n->isleafnode()
        {
            const inner_node *inner = static_cast<const inner_node*>(n);

            os.write(reinterpret_cast<const char*>(inner), sizeof(*inner));

            for(unsigned short slot = 0; slot <= inner->slotuse; ++slot)
            {
                const node *subnode = inner->childid[slot];

                dump_node(os, subnode);
            }
        }
    }

    /// Read the dump image and construct a tree from the node order in the
    /// serialization.
    node* restore_node(std::istream &is)
    {
        union {
            node        top;
            leaf_node   leaf;
            inner_node  inner;
        } nu;

        // first read only the top of the node
        is.read(reinterpret_cast<char*>(&nu.top), sizeof(nu.top));
        if (!is.good()) return NULL;

        if (nu.top.isleafnode())
        {
            // read remaining data of leaf node
            is.read(reinterpret_cast<char*>(&nu.leaf) + sizeof(nu.top), sizeof(nu.leaf) - sizeof(nu.top));
            if (!is.good()) return NULL;

            leaf_node *newleaf = allocate_leaf();

            // copy over all data, the leaf nodes contain only their double linked list pointers
            *newleaf = nu.leaf;

            // reconstruct the linked list from the order in the file
            if (headleaf == NULL) {
                BTREE_ASSERT(newleaf->prevleaf == NULL);
                headleaf = tailleaf = newleaf;
            }
            else {
                newleaf->prevleaf = tailleaf;
                tailleaf->nextleaf = newleaf;
                tailleaf = newleaf;
            }

            return newleaf;
        }
        else
        {
            // read remaining data of inner node
            is.read(reinterpret_cast<char*>(&nu.inner) + sizeof(nu.top), sizeof(nu.inner) - sizeof(nu.top));
            if (!is.good()) return NULL;

            inner_node *newinner = allocate_inner(0);

            // copy over all data, the inner nodes contain only pointers to their children
            *newinner = nu.inner;

            for(unsigned short slot = 0; slot <= newinner->slotuse; ++slot)
            {
                newinner->childid[slot] = restore_node(is);
            }

            return newinner;
        }
    }
};

} // namespace stx

#endif // _STX_BTREE_H_
