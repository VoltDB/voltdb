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

#include "structures/CompactingPool.h"
#ifdef VOLT_POOL_CHECKING
#include "common/StackTrace.h"
#include <mutex>
#endif
#include <memory>
#include "debuglog.h"
#include "boost/pool/pool.hpp"

namespace voltdb {

using CompactingStringStorage = std::unordered_map<int32_t, std::unique_ptr<CompactingPool>>;

struct voltdb_pool_allocator_new_delete {
    using size_type = std::size_t;         // These typedefs are required by boost::pool
    using difference_type = std::ptrdiff_t;
    static char* malloc(const size_t bytes);
    static void free(char * const block);
};

using PoolForObjectSize = boost::pool<voltdb_pool_allocator_new_delete>;
using PoolsByObjectSize = std::unordered_map<std::size_t, std::unique_ptr<PoolForObjectSize>>;

using PoolPairType = std::pair<int, PoolsByObjectSize*>;

struct PoolLocals {
    PoolLocals();
    PoolLocals(bool dummyEntry) { }
    PoolLocals(const PoolLocals& src) = default;

    PoolLocals& operator = (PoolLocals const& rhs) = default;

    PoolPairType* poolData = nullptr;
    CompactingStringStorage* stringData = nullptr;
    std::size_t* allocated;
    int32_t* enginePartitionId = nullptr;
};


/**
 * A wrapper around a set of pools that are local to the current thread.
 * An instance of the thread local pool must be maintained somewhere in the thread to ensure initialization
 * and destruction of the thread local pools. Creating multiple instances is fine, it is reference counted. The thread local
 * instance of pools will be freed once the last ThreadLocalPool reference in the thread is destructed.
 */
class ThreadLocalPool {
public:
    ThreadLocalPool();
    ~ThreadLocalPool();

    /// The layout of an allocation segregated by size,
    /// including overhead to help identify the size-specific
    /// pool from which the allocation must be freed.
    /// Uses placement new and a constructor to overlay onto
    /// the variable-length raw internal allocation and
    /// initialize the requested size as a prefix field.
    /// The m_data field makes it easy to access the user
    /// data at its fixed offset.
    struct Sized {
        int32_t m_size;
        char m_data[0];
        Sized(int32_t requested_size) : m_size(requested_size) { }
    };

    // This needs to be >= the VoltType.MAX_VALUE_LENGTH defined in java, currently 1048576.
    // The rationale for making it any larger would be to allow calculating wider "temp"
    // values for use in situations where they are not being stored as column values.
    static const int POOLED_MAX_VALUE_LENGTH = 1024 * 1024;

    static void assignThreadLocals(const PoolLocals& mapping);

    static PoolPairType* getDataPoolPair();

    /**
     * Allocate space from a page of objects of the requested size.
     * Each new size of object splinters the allocated memory into a new pool
     * which is a collection of pages of objects of that exact size.
     * Each pool will allocate additional space that is initially unused.
     * This is not an issue when the allocated objects will be instances of a
     * class that has many instances to quickly fill up the unused space. So,
     * an optimal use case is a custom operator new for a commonly used class.
     * Page sizes in a pool may vary as the number of required pages grows,
     * but will be bounded to 2MB or to the size of two objects if they are
     * larger than 256KB (not typical). There is no fixed upper limit to the
     * size of object that can be requested.
     * This allocation method would be a poor choice for variable-length
     * buffers whose sizes depend on user input and may be unlikely to repeat.
     * allocateRelocatable is the better fit for that use case.
     */
    static void* allocateExactSizedObject(std::size_t size);

    /**
     * Deallocate the object returned by allocateExactSizedObject.
     */
    static void freeExactSizedObject(std::size_t, void* object);

    static std::size_t getPoolAllocationSize();

    static void setPartitionIds(int32_t partitionId);
    /**
     * Get the partition id of the executing thread.  Most often this
     * is the same as getEnginePartitionId.  But when a thread is doing
     * work on behalf of another thread this is the partion id of the
     * thread actually doing the work.
     *
     * @return The partition id of the working thread.
     */
    static int32_t getThreadPartitionId();
    static int32_t getThreadPartitionIdWithNullCheck();
    /**
     * Get the partion id of the thread on whose behalf this thread is
     * working.  Generally this is the same as the value of getThreadPartitionId.
     * But if some other thread is doing work on our behalf then this is
     * the partition id of the free rider, on whose behalf the working
     * thread is working.
     *
     * @return The partition id of the free rider thread.
     */
    static int32_t getEnginePartitionId();
    static int32_t getEnginePartitionIdWithNullCheck();

    /**
     * Allocate space from a page of objects of approximately the requested
     * size. There will be relatively small gaps of unused space between the
     * objects. This is caused by aligning them to a slightly larger size.
     * This allows allocations within a pool of similarly-sized objects
     * to always fit when they are relocated to fill a hole left by a
     * deallocation. This enables continuous compaction to prevent deallocation
     * from accumulating large unused holes in the page.
     * For the relocation to work, there can only be one persistent pointer
     * to an allocation and the pointer's address must be registered with the
     * allocator so that the allocator can reset the pointer at that address
     * when its referent needs to be relocated.
     * Allocation requests of greater than 1 megabyte + 12 bytes will throw a
     * fatal exception. This limit is arbitrary and could be extended if
     * needed. The caller is expected to guard against this fatal condition.
     * This allocation method is ideal for variable-length user data that is
     * managed through a single point of reference (See class StringRef).
     * The relocation feature makes this allocation method a poor choice for
     * objects that could be referenced by multiple persistent pointers.
     * allocateExactSizedObject uses a simpler, more general allocator that
     * works well with fixed-sized allocations and counted references.
     * Also, the sole persistent pointer is assumed to remain at a fixed
     * address for the lifetime of the allocation, but it would be easy to add
     * a function that allowed the persistent pointer to be safely relocated
     * and re-registered.
     */
    static Sized* allocateRelocatable(char** referrer, int32_t sz);

    /**
     * Return the rounded-up buffer size that was allocated for the string.
     */
    static int32_t getAllocationSizeForRelocatable(Sized* string);

    /**
     * Deallocate the object returned by allocateRelocatable.
     * This implements continuous compaction which can have the side effect of
     * relocating some other allocation.
     */
    static void freeRelocatable(Sized* string);

    static void resetStateForTest();
    static int32_t* getThreadPartitionIdForTest();
    static void setThreadPartitionIdForTest(int32_t* partitionId);
#ifdef VOLT_POOL_CHECKING
    void shutdown();
#endif
private:
    #ifdef VOLT_POOL_CHECKING
        friend class SynchronizedThreadLock;
        static StackTrace* getStackTraceFor(int32_t engineId, std::size_t sz, void* object);

        int32_t m_allocatingEngine;
        int32_t m_allocatingThread;
        static std::mutex s_sharedMemoryMutex;
    #ifdef VOLT_TRACE_ALLOCATIONS
        using AllocTraceMap_t = std::unordered_map<void*, StackTrace*>;
    #else
        using AllocTraceMap_t = std::unordered_set<void*> ;
    #endif
        using SizeBucketMap_t = std::unordered_map<std::size_t, AllocTraceMap_t> ;
        using PartitionBucketMap_t = std::unordered_map<int32_t, SizeBucketMap_t> ;
        static PartitionBucketMap_t s_allocations;
        bool m_shutdown = false;
    #endif
};
}

