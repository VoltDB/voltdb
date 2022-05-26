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

#include <iterator>
#include <memory>
#include <utility>

#include "common/LargeTempTableBlockId.hpp"
#include "common/tabletuple.h"

namespace voltdb {

class TableTuple;
class TupleSchema;

/**
 * A wrapper around a buffer of memory used to store tuples.
 *
 * The lower-addressed memory of the buffer is used to store tuples of
 * fixed size, which is similar to how persistent table blocks store
 * tuples.  The higher-addressed memory stores non-inlined,
 * variable-length objects referenced in the tuples.
 *
 * As tuples are inserted into the block, both tuple and non-inlined
 * memory grow towards the middle of the buffer.  The buffer is full
 * when there is not enough room in the middle of the buffer for the
 * next tuple.
 *
 * This block layout is chosen so that the whole block may be written
 * to disk as a self-contained unit, and reloaded later (since block
 * may be at a different memory address, pointers to non-inlined data in
 * the tuples will need to be updated).
 */
class LargeTempTableBlock {

    /** the ID of this block */
    LargeTempTableBlockId m_id;

    /** the schema for the data (owned by the table) */
    const TupleSchema * m_schema;

    /** Pointer to block storage */
    std::unique_ptr<char[]> m_storage{new char [BLOCK_SIZE_IN_BYTES]};

    /** Points the address where the next tuple will be inserted */
    char* m_tupleInsertionPoint;

    /** Points to the byte after the end of the storage buffer (before
      any non-inlined data has been inserted), or to the first byte
      of the last non-inlined object that was inserted.
      I.e., m_nonInlinedInsertionPoint - [next non-inlined object size]
      is where the next non-inlined object will be inserted. */
    char* m_nonInlinedInsertionPoint;

    /** True if this object cannot be evicted from the LTT block cache
      and stored to disk */
    bool m_isPinned = false;

    /** True if this block is stored on disk (may or may not be currently resident).
      Blocks that are resident and also stored can be evicted without doing any I/O. */
    bool m_isStored = false;

    /**
     * Number of tuples currently in this block.  This is also stored in the tuple
     * block storage itself.  These two values need to be kept in sync.
     */
    int64_t m_activeTupleCount = 0;

    /** Return a pointer to the first 8-byte word in the buffer.
     * This is the original address of the buffer when it gets
     * saved and reloaded from disk.  When this word is not equal to
     * the buffers current address, string pointers in the tuples
     * must be updated to reflect the buffer's new location
     * */
    char** getStorageAddressPointer() {
        return reinterpret_cast<char**>(&m_storage[0]);
    }

    /**
     * Return the address of 4-byte integer in the block header that
     * contains the tuple count for the block.
     */
    int32_t* getStorageTupleCount() const {
        return reinterpret_cast<int32_t*>(&m_storage[sizeof(char*)]);
    }
public:
    template<bool IsConst> class LttBlockIterator;

    using iterator = LttBlockIterator<false>;
    using const_iterator = LttBlockIterator<true>;

    /** The size of all large temp table blocks.  Some notes about
        block size:
        - The maximum row size is 2MB.
        - A small block size will waste space if tuples large
        - A large block size will waste space if tables and tuples are
          small
        8MB seems like a reasonable choice since it's large enough to
        hold a few tuples of the maximum size.
    */
    static const size_t BLOCK_SIZE_IN_BYTES = 8 * 1024 * 1024; // 8 MB

    /** Each block has a header of 12 bytes:
        - 8 bytes for the address of the block in memory.  This is needed
          when loading a block from disk back into memory, to update pointers
          to non-inlined string data
        - 4 bytes for the number of tuples in the block.
        This information is redundant (this class contains a separate tuple count),
        but needed for when we serialize data to disk.
    */
    static const size_t HEADER_SIZE = 8 + 4;

    /** constructor for a new block. */
    LargeTempTableBlock(LargeTempTableBlockId id, const TupleSchema* schema);

    /** Return the unique ID for this block */
    LargeTempTableBlockId id() const {
        return m_id;
    }

    /** insert a tuple into this block.  Returns true if insertion was
        successful.  */
    bool insertTuple(const TableTuple& source);

    /** Because we can allocate non-inlined objects into LTT blocks,
        this class needs to function like a pool, and this allocate
        method provides this. */
    void* allocate(std::size_t size);

    /** Return the ordinal position of the next free slot in this
        block. */
    uint32_t unusedTupleBoundary() {
        return m_activeTupleCount;
    }

    /** Return a pointer to the storage for this block. */
    char* tupleStorage() const {
        return m_storage.get() + HEADER_SIZE;
    }

    /** Return a pointer to the storage for this block. (not const) */
    char* tupleStorage() {
        return m_storage.get() + HEADER_SIZE;
    }

    /** Returns the amount of memory used by this block.  For blocks
        that are resident (not stored to disk) this will return
        BLOCK_SIZE_IN_BYTES, and zero otherwise.
        Note that this value may not be equal to
        getAllocatedTupleMemory() + getAllocatedPoolMemory() because
        of the block header and unused space at the middle of the block. */
    int64_t getAllocatedMemory() const;

    /** Return the number of bytes used to store tuples in this
        block */
    int64_t getAllocatedTupleMemory() const;

    /** Return the number of bytes used to store non-inlined objects in
        this block. */
    int64_t getAllocatedPoolMemory() const;

    /** Release the storage associated with this block (so it can be
        persisted to disk).  Marks the block as "stored." */
    std::unique_ptr<char[]> releaseData();

    /** Set the storage associated with this block (as when loading
        from disk) */
    void setData(std::unique_ptr<char[]> storage);

    /** Returns true if this block is pinned in the cache and may not
        be stored to disk (i.e., we are currently inserting tuples
        into or iterating over the tuples in this block)  */
    bool isPinned() const {
        return m_isPinned;
    }

    /** Mark this block as pinned and un-evictable */
    void pin() {
        vassert(!m_isPinned);
        m_isPinned = true;
    }

    /** Mark this block as unpinned and evictable */
    void unpin() {
        vassert(m_isPinned);
        m_isPinned = false;
    }

    /** Returns true if this block is currently loaded into memory */
    bool isResident() const {
        return m_storage.get() != NULL;
    }

    /** Returns true if this block is stored on disk.  (May or may not
        also be resident) */
    bool isStored() const {
        return m_isStored;
    }

    void unstore() {
        m_isStored = false;
    }

    /** Return the number of tuples in this block */
    int64_t activeTupleCount() const {
        return m_activeTupleCount;
    }

    /** Return the schema of the tuples in this block */
    const TupleSchema* schema() const {
        return m_schema;
    }

    /** Swap the contents of the two blocks.  It's up to the caller to
        invalidate any copies of this block on disk. */
    void swap(LargeTempTableBlock* otherBlock) {
        vassert(m_schema->isCompatibleForMemcpy(otherBlock->m_schema));
        // id should stay the same
        // m_schema is the same
        m_storage.swap(otherBlock->m_storage);
        std::swap(m_tupleInsertionPoint, otherBlock->m_tupleInsertionPoint);
        std::swap(m_nonInlinedInsertionPoint, otherBlock->m_nonInlinedInsertionPoint);
        std::swap(m_activeTupleCount, otherBlock->m_activeTupleCount);
    }

    /** Clear all the data out of this block. */
    void clearForTest() {
        m_tupleInsertionPoint = tupleStorage();
        m_nonInlinedInsertionPoint = m_storage.get() + BLOCK_SIZE_IN_BYTES;
        m_activeTupleCount = 0;
    }

    LargeTempTableBlock::iterator begin();
    LargeTempTableBlock::const_iterator begin() const;
    LargeTempTableBlock::const_iterator cbegin() const;

    LargeTempTableBlock::iterator end();
    LargeTempTableBlock::const_iterator end() const;
    LargeTempTableBlock::const_iterator cend() const;

    /** This debug method will skip printing non-inlined strings (will
        just print their address) to avoid a SEGV when debugging. */
    std::string debug() const;

    /** This debug method will print non-inlined strings, which could
        cause a crash if the StringRef pointer is invalid. */
    std::string debugUnsafe() const;

    struct Tuple {
        char m_statusByte;
        char m_tupleData[];

        TableTuple toTableTuple(const TupleSchema* schema) {
            return TableTuple(reinterpret_cast<char*>(this), schema);
        }

        const TableTuple toTableTuple(const TupleSchema* schema) const {
            return TableTuple(reinterpret_cast<char*>(const_cast<Tuple*>(this)), schema);
        }

        Tuple(const Tuple&) = delete;
        Tuple& operator=(const Tuple&) = delete;
    };
};

template<bool IsConst>
class LargeTempTableBlock::LttBlockIterator {
    int m_tupleLength;
    char* m_tupleAddress;
public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = LargeTempTableBlock::Tuple;
    using difference_type = std::ptrdiff_t;
    using reference = typename std::conditional<IsConst, const value_type&, value_type&>::type;
    using pointer = typename std::conditional<IsConst, const value_type*, value_type*>::type;

    LttBlockIterator() : m_tupleLength(0) , m_tupleAddress(NULL) { }

     LttBlockIterator(const TupleSchema* schema, char* storage)
        : m_tupleLength(schema->tupleLength() + TUPLE_HEADER_SIZE) , m_tupleAddress(storage) { }

     LttBlockIterator(int tupleLength, char* storage)
         : m_tupleLength(tupleLength) , m_tupleAddress(storage) { }

    // You can convert a regular iterator to a const_iterator
    operator LttBlockIterator<true>() const {
        return LttBlockIterator<true>(m_tupleLength, m_tupleAddress);
    }

    bool operator==(const LttBlockIterator& that) const {
        return m_tupleAddress == that.m_tupleAddress;
    }

    bool operator!=(const LttBlockIterator& that) const {
        return m_tupleAddress != that.m_tupleAddress;
    }

    reference operator*() {
        return *reinterpret_cast<pointer>(m_tupleAddress);
    }

    pointer operator->() {
        return reinterpret_cast<pointer>(m_tupleAddress);
    }

    // pre-increment
    LttBlockIterator& operator++() {
        m_tupleAddress += m_tupleLength;
        return *this;
    }

    // post-increment
    LttBlockIterator operator++(int) {
        LttBlockIterator orig = *this;
        ++(*this);
        return orig;
    }

    // pre-decrement
    LttBlockIterator& operator--() {
        m_tupleAddress -= m_tupleLength;
        return *this;
    }

    // post-decrement
    LttBlockIterator operator--(int) {
        LttBlockIterator orig = *this;
        --(*this);
        return orig;
    }

    LttBlockIterator& operator+=(difference_type n) {
        m_tupleAddress += n * m_tupleLength;
        return *this;
    }

    LttBlockIterator& operator-=(difference_type n) {
        m_tupleAddress -= n * m_tupleLength;
        return *this;
    }

    LttBlockIterator operator+(difference_type n) const {
        LttBlockIterator it{*this};
        it += n;
        return it;
    }

    LttBlockIterator operator-(difference_type n) const {
        LttBlockIterator it{*this};
        it -= n;
        return it;
    }

    difference_type operator-(const LttBlockIterator& that) const {
        std::ptrdiff_t ptrdiff = m_tupleAddress - that.m_tupleAddress;
        return ptrdiff / m_tupleLength;
    }

    reference operator[](difference_type n) {
        LttBlockIterator temp{*this + n};
        return *temp;
    }

    // relational operators
    bool operator>(const LttBlockIterator& that) const {
        return m_tupleAddress > that.m_tupleAddress;
    }

    bool operator<(const LttBlockIterator& that) const {
        return m_tupleAddress < that.m_tupleAddress;
    }

    bool operator>=(const LttBlockIterator& that) const {
        return m_tupleAddress >= that.m_tupleAddress;
    }

    bool operator<=(const LttBlockIterator& that) const {
        return m_tupleAddress <= that.m_tupleAddress;
    }

};

template<bool IsConst>
inline LargeTempTableBlock::LttBlockIterator<IsConst> operator+(
        typename LargeTempTableBlock::LttBlockIterator<IsConst>::difference_type n,
        LargeTempTableBlock::LttBlockIterator<IsConst> const& it) {
    return it + n;
}

} // end namespace voltdb

