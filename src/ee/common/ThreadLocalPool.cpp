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

#include "common/ThreadLocalPool.h"

#include "common/FatalException.hpp"
#include "common/SQLException.h"
#include "common/SynchronizedThreadLock.h"
#include "ExecuteWithMpMemory.h"

#include <iostream>
#include <pthread.h>

namespace voltdb {

#if defined (MEMCHECK) && defined (VOLT_POOL_CHECKING)
#error Do not build with both MEMCHECK and VOLT_POOL_CHECKING turned on
#endif


/**
 * Thread local key for storing thread specific memory pools
 */
namespace {
pthread_key_t m_key;
pthread_key_t m_stringKey;
/**
 * Thread local key for storing integer value of amount of memory allocated
 */
pthread_key_t m_allocatedKey;
pthread_key_t m_threadPartitionIdKey;
pthread_key_t m_enginePartitionIdKey;
pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

}

#ifdef VOLT_POOL_CHECKING
pthread_mutex_t ThreadLocalPool::s_sharedMemoryMutex = PTHREAD_MUTEX_INITIALIZER;
ThreadLocalPool::PartitionBucketMap_t ThreadLocalPool::s_allocations;
#endif

namespace {
void createThreadLocalKey() {
    (void) pthread_key_create(&m_key, NULL);
    (void) pthread_key_create(&m_stringKey, NULL);
    (void) pthread_key_create(&m_allocatedKey, NULL);
    (void) pthread_key_create(&m_threadPartitionIdKey, NULL);
    (void) pthread_key_create(&m_enginePartitionIdKey, NULL);
}
}

ThreadLocalPool::ThreadLocalPool()
{
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    if (pthread_getspecific(m_key) == NULL) {
        pthread_setspecific(m_allocatedKey, static_cast<const void *>(new std::size_t(0)));
        // Since these are int32_t values we can't just
        // put them into the void* pointer which is the thread
        // local data.  We have to allocate an int32_t
        // buffer to hold the partition id value.
        pthread_setspecific(m_threadPartitionIdKey, static_cast<const void *>(new int32_t(0)));
        pthread_setspecific(m_enginePartitionIdKey, static_cast<const void *>(new int32_t(0)));
        PoolsByObjectSize* pools = new PoolsByObjectSize();
        PoolPairType* refCountedPools = new PoolPairType(1, pools);
        pthread_setspecific(m_key, static_cast<const void *>(refCountedPools));
        pthread_setspecific(m_stringKey, static_cast<const void*>(new CompactingStringStorage()));
#ifdef VOLT_POOL_CHECKING
        m_allocatingEngine = -1;
        m_allocatingThread = -1;
#endif
    } else {
        PoolPairTypePtr p =
                static_cast<PoolPairTypePtr>(pthread_getspecific(m_key));
        p->first++;
        VOLT_TRACE("Increment (%d) ThreadPool Memory counter for partition %d on thread %d",
                p->first, getEnginePartitionId(), getThreadPartitionId());
#ifdef VOLT_POOL_CHECKING
        m_allocatingEngine = getEnginePartitionId();
        m_allocatingThread = getThreadPartitionId();
#endif
    }
}

ThreadLocalPool::~ThreadLocalPool() {
    PoolPairTypePtr p = static_cast<PoolPairTypePtr>(pthread_getspecific(m_key));
    if (p == NULL) {
        VOLT_ERROR("Failed to find context");
        VOLT_ERROR_STACK();
        assert(p != NULL);
    }
    if (p != NULL) {
        if (p->first == 1) {
            delete p->second;
            pthread_setspecific( m_key, NULL);
            delete static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
            pthread_setspecific(m_stringKey, NULL);
            delete static_cast<std::size_t*>(pthread_getspecific(m_allocatedKey));
            pthread_setspecific( m_allocatedKey, NULL);
            int32_t* threadPartitionIdPtr = static_cast<int32_t*>(pthread_getspecific(m_threadPartitionIdKey));
            pthread_setspecific( m_threadPartitionIdKey, NULL);
            int32_t* enginePartitionIdPtr = static_cast<int32_t*>(pthread_getspecific(m_enginePartitionIdKey));
#ifdef VOLT_POOL_CHECKING
            VOLT_TRACE("Destroying ThreadPool Memory for partition %d on thread %d", *enginePartitionIdPtr, *threadPartitionIdPtr);
            // Sadly, a delta table is created on demand and deleted using a refcount so it is likely for it to be created on the lowest partition
            // but deallocated on partition that cleans up the last view handler so we can't enforce thread-based allocation validation below:
            // if (m_allocatingThread != -1 && (*threadPartitionIdPtr != m_allocatingThread || *enginePartitionIdPtr != m_allocatingEngine)) {
            if (m_allocatingThread != -1 && *enginePartitionIdPtr != m_allocatingEngine) {
                // Only the VoltDBEngine's ThreadLocalPool instance will have a -1 allocating thread because the threadId
                // has not been assigned yet. Normally the last ThreadLocalPool instance to be deallocated is the VoltDBEngine.
                VOLT_ERROR("Unmatched deallocation allocated from partition %d on thread %d", m_allocatingEngine, m_allocatingThread);
                VOLT_ERROR("deallocation from:");
                VOLT_ERROR_STACK();
                assert(false);
            }
            pthread_mutex_lock(&s_sharedMemoryMutex);
            SizeBucketMap_t& mapBySize = s_allocations[*enginePartitionIdPtr];
            pthread_mutex_unlock(&s_sharedMemoryMutex);
            SizeBucketMap_t::iterator mapForAdd = mapBySize.begin();
            while (mapForAdd != mapBySize.end()) {
                AllocTraceMap_t& allocMap = mapForAdd->second;
                mapForAdd++;
                if (!allocMap.empty()) {
                    AllocTraceMap_t::iterator nextAlloc = allocMap.begin();
                    do {
#ifdef VOLT_TRACE_ALLOCATIONS
                        VOLT_ERROR("Missing deallocation for %p at:", nextAlloc->first);
                        nextAlloc->second->printLocalTrace();
                        delete nextAlloc->second;
#else
                        VOLT_ERROR("Missing deallocation for %p at:", *nextAlloc);
#endif
                        nextAlloc++;
                    } while (nextAlloc != allocMap.end());
                    allocMap.clear();
                    assert(false);
                }
                mapBySize.erase(mapBySize.begin());
            }
#endif
            if (threadPartitionIdPtr) {
                SynchronizedThreadLock::resetMemory(*threadPartitionIdPtr);
            }
            delete threadPartitionIdPtr;
            delete enginePartitionIdPtr;
            delete p;
        } else {
            p->first--;
#ifdef VOLT_POOL_CHECKING
            VOLT_TRACE("Decrement (%d) ThreadPool Memory counter for partition %d on thread %d",
                    p->first, getEnginePartitionId(), getThreadPartitionId());
            // Sadly, a delta table is created on demand and deleted using a refcount so it is likely for it to be created on the lowest partition
            // but deallocated on partition that cleans up the last view handler so we can't enforce thread-based allocation validation below:
            // if (m_allocatingThread != -1 && (getThreadPartitionId() != m_allocatingThread || getEnginePartitionId() != m_allocatingEngine)) {
            if (m_allocatingThread != -1 && getEnginePartitionId() != m_allocatingEngine) {
                VOLT_ERROR("Unmatched deallocation allocated from partition %d on thread %d", m_allocatingEngine, m_allocatingThread);
                VOLT_ERROR("deallocation from partition %d on thread %d:", getEnginePartitionId(), getThreadPartitionId());
                VOLT_ERROR_STACK();
                assert(false);
            }
#endif
        }
    }
}

void ThreadLocalPool::assignThreadLocals(const PoolLocals& mapping)
{
    assert(mapping.enginePartitionId != NULL && getThreadPartitionId() != 16383);

    pthread_setspecific(m_allocatedKey, static_cast<const void *>(mapping.allocated));
    pthread_setspecific(m_key, static_cast<const void *>(mapping.poolData));
    pthread_setspecific(m_stringKey, static_cast<const void*>(mapping.stringData));
    pthread_setspecific(m_enginePartitionIdKey, static_cast<const void*>(mapping.enginePartitionId));
}

void ThreadLocalPool::resetStateForTest() {
    pthread_setspecific(m_allocatedKey, NULL);
    pthread_setspecific(m_key, NULL);
    pthread_setspecific(m_stringKey, NULL);
    pthread_setspecific(m_enginePartitionIdKey, NULL);
    pthread_setspecific(m_threadPartitionIdKey, NULL);
}
int32_t* ThreadLocalPool::getThreadPartitionIdForTest() {
    return static_cast< int32_t* >(pthread_getspecific(m_threadPartitionIdKey));
}
void ThreadLocalPool::setThreadPartitionIdForTest(int32_t* partitionId) {
    pthread_setspecific(m_threadPartitionIdKey, static_cast<const void*>(partitionId));
}

namespace {
int32_t getAllocationSizeForObject(int length) {
    static const int32_t NVALUE_LONG_OBJECT_LENGTHLENGTH = 4;
    static const int32_t MAX_ALLOCATION = ThreadLocalPool::POOLED_MAX_VALUE_LENGTH +
                                          NVALUE_LONG_OBJECT_LENGTHLENGTH +
                                          CompactingPool::FIXED_OVERHEAD_PER_ENTRY();

    int length_to_fit = length +
                        NVALUE_LONG_OBJECT_LENGTHLENGTH +
                        CompactingPool::FIXED_OVERHEAD_PER_ENTRY();

    // The -1 and repeated shifting and + 1 are part of the rounding algorithm
    // that produces the nearest power of 2 greater than or equal to the value.
    int target = length_to_fit - 1;
    target |= target >> 1;
    target |= target >> 2;
    target |= target >> 4;
    target |= target >> 8;
    target |= target >> 16;
    target++;
    // Try to shrink the target to "midway" down to the previous power of 2,
    // if the length fits.
    // Strictly speaking, a geometric mean (dividing the even power by sqrt(2))
    // would give a more consistently proportional over-allocation for values
    // at slightly different scales, but the arithmetic mean (3/4 of the power)
    // is fast to calculate and close enough for our purposes.
    int threeQuartersTarget = target - (target >> 2);
    if (length_to_fit < threeQuartersTarget) {
        target = threeQuartersTarget;
    }
    if (target <= MAX_ALLOCATION) {
        return target;
    }
    if (length_to_fit <= MAX_ALLOCATION) {
        return MAX_ALLOCATION;
    }
    throwFatalException("Attempted to allocate an object larger than the 1 MB limit. Requested size was %d",
                        length);
}
}

int TestOnlyAllocationSizeForObject(int length)
{
    return getAllocationSizeForObject(length);
}


#ifdef MEMCHECK
/// Persistent string pools with their compaction are completely bypassed for
/// the memcheck build. It just does standard C++ heap allocations and
/// deallocations.
ThreadLocalPool::Sized* ThreadLocalPool::allocateRelocatable(char** referrer_ignored, int32_t sz)
{
    return new (new char[sizeof(Sized) + sz]) Sized(sz);
}

int32_t ThreadLocalPool::getAllocationSizeForRelocatable(Sized* data)
{
    return static_cast<int32_t>(data->m_size + sizeof(Sized));
}

void ThreadLocalPool::freeRelocatable(Sized* data)
{ delete [] reinterpret_cast<char*>(data); }

#else // not MEMCHECK

PoolPairTypePtr ThreadLocalPool::getDataPoolPair()
{
    return static_cast< PoolPairTypePtr >(pthread_getspecific(m_key));
}

namespace {
CompactingStringStorage &getStringPoolMap() {
    return *static_cast<CompactingStringStorage *>(pthread_getspecific(m_stringKey));
}
}

ThreadLocalPool::Sized* ThreadLocalPool::allocateRelocatable(char** referrer, int32_t sz)
{
    // The size provided to this function determines the
    // approximate-size-specific pool selection. It gets
    // reflected (after rounding and padding) in the size
    // prefix padded into each allocation. The size prefix is somewhat
    // redundant with the "object length" that NValue will eventually
    // encode into the first 1-3 bytes of the buffer being returned here.
    // So, in theory, this code could avoid adding the overhead of a
    // "Sized" allocation by trusting the NValue code and decoding
    // (and rounding up) the object length out of the first few bytes
    // of the "user data" whenever it gets passed back into
    // getAllocationSizeForRelocatable and freeRelocatable.
    // For now, to keep the allocator simple and abstract,
    // NValue and the allocator each keep their own accounting.
    int32_t alloc_size = getAllocationSizeForObject(sz);
    CompactingStringStorage& poolMap = getStringPoolMap();
    //std::cerr << " *** Allocating from pool " << &poolMap << std::endl;
    CompactingStringStorage::iterator iter = poolMap.find(alloc_size);
    void* allocation;
    if (iter == poolMap.end()) {
        // There is no pool yet for objects of this size, so create one.
        // Compute num_elements to be the largest multiple of alloc_size
        // to fit in a 2MB buffer.
        int32_t num_elements = ((2 * 1024 * 1024 - 1) / alloc_size) + 1;
        boost::shared_ptr<CompactingPool> pool(new CompactingPool(alloc_size, num_elements));
        poolMap.insert(std::pair<int32_t, boost::shared_ptr<CompactingPool> >(alloc_size, pool));
        allocation = pool->malloc(referrer);
    }
    else {
        allocation = iter->second->malloc(referrer);
    }

    // Convert from the raw allocation to the initialized size header.
    Sized* sized = new (allocation) Sized(sz);
    return sized;
}

int32_t ThreadLocalPool::getAllocationSizeForRelocatable(Sized* sized)
{
    // Convert from the caller data to the size-prefixed allocation to
    // extract its size field.
    return getAllocationSizeForObject(sized->m_size);
}

void ThreadLocalPool::freeRelocatable(Sized* sized)
{
    // use the cached size to find the right pool.
    int32_t alloc_size = getAllocationSizeForObject(sized->m_size);
    CompactingStringStorage& poolMap = getStringPoolMap();
    //std::cerr << " *** Deallocating from pool " << &poolMap << std::endl;
    CompactingStringStorage::iterator iter = poolMap.find(alloc_size);
    if (iter == poolMap.end()) {
        // If the pool can not be found, there could not have been a prior
        // allocation for any object of this size, so either the caller
        // passed a bogus data pointer that was never allocated here OR
        // the data pointer's size header has been corrupted.
#ifdef VOLT_POOL_CHECKING
        // We will catch this when we see what compacting pool data is left
        VOLT_ERROR("Deallocated relocatable pointer %p in wrong context thread (partition %d)",
                sized, getEnginePartitionId());
        VOLT_ERROR_STACK();
#else
        throwFatalException("Attempted to free an object of an unrecognized size. Requested size was %d",
                            alloc_size);
#endif
    }
    // Free the raw allocation from the found pool.
    iter->second->free(sized);
}

#endif

void* ThreadLocalPool::allocateExactSizedObject(std::size_t sz)
{
    PoolsByObjectSize& pools =
            *(static_cast< PoolPairTypePtr >(pthread_getspecific(m_key))->second);
    PoolsByObjectSize::iterator iter = pools.find(sz);
    PoolForObjectSize* pool;
#ifdef VOLT_POOL_CHECKING
    int32_t enginePartitionId =  getEnginePartitionId();
    pthread_mutex_lock(&s_sharedMemoryMutex);
    SizeBucketMap_t& mapBySize = s_allocations[enginePartitionId];
    pthread_mutex_unlock(&s_sharedMemoryMutex);
    SizeBucketMap_t::iterator mapForAdd;
#endif
    if (iter == pools.end()) {
        pool = new PoolForObjectSize(sz);
        PoolForObjectSizePtr poolPtr(pool);
        pools.insert(std::pair<std::size_t, PoolForObjectSizePtr>(sz, poolPtr));
#ifdef VOLT_POOL_CHECKING
        mapForAdd = mapBySize.find(sz);
        if (mapForAdd == mapBySize.end()) {
            mapForAdd = mapBySize.insert(std::make_pair(sz, AllocTraceMap_t())).first;
        }
        else {
            assert(mapForAdd->second.size() == 0);
        }
#endif
    }
    else {
        pool = iter->second.get();
#ifdef VOLT_POOL_CHECKING
        mapForAdd = mapBySize.find(sz);
        assert(mapForAdd != mapBySize.end());
#endif
    }
    /**
     * The goal of this code is to bypass the pool sizing algorithm used by
     * boost and replace it with something that bounds allocations to a series
     * of 2MB blocks for small allocation sizes. For larger allocations
     * (not a typical case, possibly not a useful case), fall back to
     * allocating two of these huge things at a time.
     * The goal of this bounding is to make the amount of unused but allocated
     * memory relatively small so that the counting done by the volt allocator
     * accurately represents the effect on RSS. Left to its own algorithms,
     * boost will purposely allocate pages that increase in size until they
     * are too large to ever overflow, regardless of absolute scale.
     * That makes it likely that they will contain lots of unused space
     * (for safety against repeated allocations). VoltDB prefers
     * to risk lots of separate smaller allocations (~2MB each) at larger
     * scale rather than risk fewer, larger, but mostly unused buffers.
     * Also, for larger allocation requests (not typical -- not used? -- in
     * VoltDB, boost will _start_ with very large blocks, while VoltDB would
     * prefer to start smaller with just 2 allocations per block.
     */
    if (pool->get_next_size() * pool->get_requested_size() > (1024 * 1024 * 2)) {
        //If the size of objects served by this pool is less than 256 kilobytes
        // plan to allocate a 2MB block, but no larger, even if it eventually
        // requires more blocks than boost would normally allocate.
        if (pool->get_requested_size() < (1024 * 256)) {
            pool->set_next_size((1024 * 1024 * 2) /  pool->get_requested_size());
        } else {
            //For large objects allocated just two of them
            pool->set_next_size(2);
        }
    }
#ifdef VOLT_POOL_CHECKING
    void* newMem = pool->malloc();
#ifdef VOLT_TRACE_ALLOCATIONS
    StackTrace* st = new StackTrace();
    bool success = mapForAdd->second.insert(std::make_pair(newMem, st)).second;
#else
    bool success = mapForAdd->second.insert(newMem).second;
#endif
    if (!success) {
        VOLT_ERROR("Previously allocated (see below) pointer %p is being allocated a second time on thread (partition %d)",
                newMem, getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
        mapForAdd->second[newMem]->printLocalTrace();
        delete st;
#endif
        assert(false);
    }
    VOLT_DEBUG("Allocated %p of size %lu on engine %d, thread %d", newMem, sz,
            getEnginePartitionId(), getThreadPartitionId());
    return newMem;
#else
    return pool->malloc();
#endif
}

#ifdef VOLT_POOL_CHECKING
StackTrace* ThreadLocalPool::getStackTraceFor(int32_t engineId, std::size_t sz, void* object) {
#ifdef VOLT_TRACE_ALLOCATIONS
    pthread_mutex_lock(&s_sharedMemoryMutex);
    PartitionBucketMap_t::iterator foundEngineAlloc = s_allocations.find(engineId);
    pthread_mutex_unlock(&s_sharedMemoryMutex);
    if (foundEngineAlloc == s_allocations.end()) {
        return NULL;
    }
    SizeBucketMap_t::iterator mapForAdd = foundEngineAlloc->second.find(sz);
    if (mapForAdd == foundEngineAlloc->second.end()) {
        return NULL;
    }
    else {
        AllocTraceMap_t::iterator alloc = mapForAdd->second.find(object);
        if (alloc == mapForAdd->second.end()) {
            return NULL;
        }
        else {
           return alloc->second;
        }
    }
#else
    return NULL;
#endif
}
#endif

void ThreadLocalPool::freeExactSizedObject(std::size_t sz, void* object)
{
#ifdef VOLT_POOL_CHECKING
    int32_t engineId = getEnginePartitionId();
    VOLT_DEBUG("Deallocating %p of size %lu on engine %d, thread %d", object, sz,
            engineId, getThreadPartitionId());
    pthread_mutex_lock(&s_sharedMemoryMutex);
    SizeBucketMap_t& mapBySize = s_allocations[engineId];
    pthread_mutex_unlock(&s_sharedMemoryMutex);
    SizeBucketMap_t::iterator mapForAdd = mapBySize.find(sz);
    if (mapForAdd == mapBySize.end()) {
        VOLT_ERROR("Deallocated data pointer %p in wrong context thread (partition %d)",
                object, engineId);
        VOLT_ERROR_STACK();
        if (engineId == SynchronizedThreadLock::s_mpMemoryPartitionId) {
            StackTrace* st = getStackTraceFor(0, sz, object);
            if (st) {
                VOLT_ERROR("Allocated data partition %d:", 0);
                st->printLocalTrace();
            }
        }
        else {
            StackTrace* st = getStackTraceFor(SynchronizedThreadLock::s_mpMemoryPartitionId, sz, object);
            if (st) {
                VOLT_ERROR("Allocated data partition %d:", SynchronizedThreadLock::s_mpMemoryPartitionId);
                st->printLocalTrace();
            }
        }
        throwFatalException("Attempt to deallocate exact-sized object of unknown size");
    }
    else {
        AllocTraceMap_t::iterator alloc = mapForAdd->second.find(object);
        if (alloc == mapForAdd->second.end()) {
            VOLT_ERROR("Deallocated data pointer %p in wrong context thread (partition %d)",
                    object, engineId);
            VOLT_ERROR_STACK();
            if (engineId == SynchronizedThreadLock::s_mpMemoryPartitionId) {
                StackTrace* st = getStackTraceFor(0, sz, object);
                if (st) {
                    VOLT_ERROR("Allocated data partition %d:", 0);
                    st->printLocalTrace();
                }
            }
            else {
                StackTrace* st = getStackTraceFor(SynchronizedThreadLock::s_mpMemoryPartitionId, sz, object);
                if (st) {
                    VOLT_ERROR("Allocated data partition %d:", SynchronizedThreadLock::s_mpMemoryPartitionId);
                    st->printLocalTrace();
                }
            }
            throwFatalException("Attempt to deallocate unknown exact-sized object");
            return;
        }
        else {
#ifdef VOLT_TRACE_ALLOCATIONS
           free(alloc->second);
#endif
            mapForAdd->second.erase(alloc);
        }
    }
#endif

    PoolsByObjectSize& pools =
            *(static_cast< PoolPairTypePtr >(pthread_getspecific(m_key))->second);
    PoolsByObjectSize::iterator iter = pools.find(sz);
    if (iter == pools.end()) {
        throwFatalException(
                "Failed to locate an allocated object of size %ld to free it.",
                static_cast<long>(sz));
    }
    PoolForObjectSize* pool = iter->second.get();
    pool->free(object);
}

// internal non-member helper function for calcuate Pool allocation Size
std::size_t getPoolAllocationSize_internal(size_t *bytes, CompactingStringStorage *poolMap){
    size_t bytes_allocated = *bytes;
    // For relocatable objects, each object-size-specific pool
    // -- or actually, its ContiguousAllocator -- tracks its own memory
    // allocation, so sum them, here.
    for (CompactingStringStorage::iterator iter = poolMap->begin();
         iter != poolMap->end(); ++iter) {
        bytes_allocated += iter->second->getBytesAllocated();
    }
    return bytes_allocated;
}

std::size_t ThreadLocalPool::getPoolAllocationSize() {
    size_t bytes_allocated = getPoolAllocationSize_internal(
            static_cast< std::size_t * >(pthread_getspecific(m_allocatedKey)),
            static_cast<CompactingStringStorage *>(pthread_getspecific(m_stringKey)));

    if (SynchronizedThreadLock::isLowestSiteContext()) {
        PoolLocals mpMapping = SynchronizedThreadLock::getMpEngine();
        bytes_allocated += getPoolAllocationSize_internal(mpMapping.allocated, mpMapping.stringData);
    }
    return bytes_allocated;
}

void ThreadLocalPool::setPartitionIds(int32_t partitionId) {
#ifdef VOLT_POOL_CHECKING
    // Don't track allocations on the mp thread because it is not used at all
    if (partitionId != 16383) {
        pthread_mutex_lock(&s_sharedMemoryMutex);
        PartitionBucketMap_t::iterator it = s_allocations.find(partitionId);
        if (it != s_allocations.end()) {
            SizeBucketMap_t& mapBySize = it->second;
            SizeBucketMap_t::iterator it2 = mapBySize.begin();
            while (it2 != mapBySize.end()) {
                assert(it2->second.empty());
                it2++;
                mapBySize.erase(mapBySize.begin());
            }
        }
        else {
            s_allocations.insert(std::make_pair(partitionId, SizeBucketMap_t()));
        }
        pthread_mutex_unlock(&s_sharedMemoryMutex);
    }
#endif
    int32_t* pidPtr =
        static_cast< int32_t* >(pthread_getspecific(m_threadPartitionIdKey));
    *pidPtr = partitionId;
    pidPtr = static_cast< int32_t* >(pthread_getspecific(m_enginePartitionIdKey));
    *pidPtr = partitionId;
}

int32_t ThreadLocalPool::getThreadPartitionId() {
    int32_t partitionId =
        *static_cast< int32_t* >(pthread_getspecific(m_threadPartitionIdKey));
    return partitionId;
}

int32_t ThreadLocalPool::getEnginePartitionId() {
    int32_t partitionId =
        *static_cast< int32_t* >(pthread_getspecific(m_enginePartitionIdKey));
    return partitionId;
}

int32_t ThreadLocalPool::getThreadPartitionIdWithNullCheck() {
    int32_t *ptrToPartitionId = static_cast< int32_t* >(pthread_getspecific(m_threadPartitionIdKey));
    if (ptrToPartitionId == NULL) {
        return -1;
    }

    return *ptrToPartitionId;
}

int32_t ThreadLocalPool::getEnginePartitionIdWithNullCheck() {
    int32_t *ptrToPartitionId = static_cast< int32_t* >(pthread_getspecific(m_enginePartitionIdKey));
    if (ptrToPartitionId == NULL) {
        return -1;
    }

    return *ptrToPartitionId;
}

char * voltdb_pool_allocator_new_delete::malloc(const size_type bytes) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_allocatedKey))) += bytes + sizeof(std::size_t);
    char *retval = new (std::nothrow) char[bytes + sizeof(std::size_t)];
    *reinterpret_cast<std::size_t*>(retval) = bytes + sizeof(std::size_t);
    return &retval[sizeof(std::size_t)];
}

void voltdb_pool_allocator_new_delete::free(char * const block) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_allocatedKey))) -= *reinterpret_cast<std::size_t*>(block - sizeof(std::size_t));
    delete [](block - sizeof(std::size_t));
}

PoolLocals::PoolLocals() {
    allocated = static_cast< std::size_t* >(pthread_getspecific(m_allocatedKey));
    poolData = static_cast< PoolPairTypePtr >(pthread_getspecific(m_key));
    stringData = static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
    enginePartitionId = static_cast< int32_t* >(pthread_getspecific(m_enginePartitionIdKey));
}

}
