/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#ifndef VOLTDB_LARGETEMPTABLEBLOCK_HPP
#define VOLTDB_LARGETEMPTABLEBLOCK_HPP

#include <memory>
#include <utility>

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
 public:

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

    /** constructor for a new block. */
    LargeTempTableBlock(int64_t id, TupleSchema* schema);

    /** Return the unique ID for this block */
    int64_t id() const {
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
    char* address() {
        return m_storage.get();
    }

    /** Returns the amount of memory used by this block.  For blocks
        that are resident (not stored to disk) this will return
        BLOCK_SIZE_IN_BYTES, and zero otherwise.
        Note that this value may not be equal to
        getAllocatedTupleMemory() + getAllocatedPoolMemory() because
        of unused space at the middle of the block. */
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
    void setData(char* origAddress, std::unique_ptr<char[]> storage);

    /** Returns true if this block is pinned in the cache and may not
        be stored to disk (i.e., we are currently inserting tuples
        into or iterating over the tuples in this block)  */
    bool isPinned() const {
        return m_isPinned;
    }

    /** Mark this block as pinned and un-evictable */
    void pin() {
        assert(!m_isPinned);
        m_isPinned = true;
    }

    /** Mark this block as unpinned and evictable */
    void unpin() {
        assert(m_isPinned);
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

    /** Return the number of tuples in this block */
    int64_t activeTupleCount() const {
        return m_activeTupleCount;
    }

    /** Return the schema of the tuples in this block */
    const TupleSchema* schema() const {
        return m_schema;
    }

    /** Return the schema of the tuples in this block (non-const version) */
    TupleSchema* schema() {
        return m_schema;
    }

    /** This debug method will skip printing non-inlined strings (will
        just print their address) to avoid a SEGV when debugging. */
    std::string debug() const;

    /** This debug method will print non-inlined strings, which could
        cause a crash if the StringRef pointer is invalid. */
    std::string debugUnsafe() const;

 private:

    /** the ID of this block */
    int64_t m_id;

    /** the schema for the data (owned by the table) */
    TupleSchema * m_schema;

    /** Pointer to block storage */
    std::unique_ptr<char[]> m_storage;

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
    bool m_isPinned;

    /** True if this block is stored on disk (may or may not be currently resident).
        Blocks that are resident and also stored can be evicted without doing any I/O. */
    bool m_isStored;

    /** Number of tuples currently in this block */
    int64_t m_activeTupleCount;
};

} // end namespace voltdb

#endif // VOLTDB_LARGETEMPTABLEBLOCK_HPP
