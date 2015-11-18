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
#include "common/ThreadLocalPool.h"

#include "common/FatalException.hpp"
#include "common/SQLException.h"

#include "structures/CompactingPool.h"

#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include <iostream>
#include <pthread.h>

namespace voltdb {

struct voltdb_pool_allocator_new_delete
{
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static char * malloc(const size_type bytes);
    static void free(char * const block);
};

// This needs to be >= the VoltType.MAX_VALUE_LENGTH defined in java, currently 1048576.
// The rationale for making it any larger would be to allow calculating wider "temp"
// values for use in situations where they are not being stored as column values.
const int ThreadLocalPool::POOLED_MAX_VALUE_LENGTH = 1024 * 1024;

/**
 * Thread local key for storing thread specific memory pools
 */
static pthread_key_t m_key;
static pthread_key_t m_stringKey;
/**
 * Thread local key for storing integer value of amount of memory allocated
 */
static pthread_key_t m_keyAllocated;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

typedef boost::pool<voltdb_pool_allocator_new_delete> PoolForObjectSize;
typedef boost::shared_ptr<PoolForObjectSize> PoolForObjectSizePtr;
typedef boost::unordered_map<std::size_t, PoolForObjectSizePtr> PoolsByObjectSize;

typedef std::pair<int, PoolsByObjectSize* > PairType;
typedef PairType* PairTypePtr;

typedef boost::unordered_map<int32_t, boost::shared_ptr<CompactingPool> > CompactingStringStorage;

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
    (void)pthread_key_create( &m_stringKey, NULL);
    (void)pthread_key_create( &m_keyAllocated, NULL);
}

ThreadLocalPool::ThreadLocalPool() {
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    if (pthread_getspecific(m_key) == NULL) {
        pthread_setspecific( m_keyAllocated, static_cast<const void *>(new std::size_t(0)));
        pthread_setspecific( m_key, static_cast<const void *>(
                new PairType(
                        1, new PoolsByObjectSize())));
        pthread_setspecific(m_stringKey, static_cast<const void*>(new CompactingStringStorage()));
    } else {
        PairTypePtr p =
                static_cast<PairTypePtr>(pthread_getspecific(m_key));
        pthread_setspecific( m_key, new PairType( p->first + 1, p->second));
        delete p;
    }
}

ThreadLocalPool::~ThreadLocalPool() {
    PairTypePtr p =
            static_cast<PairTypePtr>(pthread_getspecific(m_key));
    assert(p != NULL);
    if (p != NULL) {
        if (p->first == 1) {
            delete p->second;
            pthread_setspecific( m_key, NULL);
            delete static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
            pthread_setspecific(m_stringKey, NULL);
            delete static_cast<std::size_t*>(pthread_getspecific(m_keyAllocated));
            pthread_setspecific( m_keyAllocated, NULL);
        } else {
            pthread_setspecific( m_key, new PairType( p->first - 1, p->second));
        }
        delete p;
    }
}

static int32_t getAllocationSizeForObject(int length)
{
    // This is an unfortunate abstraction leak.
    // This allocator helper function happens to know not only the max length
    // of a persistent string value but also the max number of prefix length
    // bytes that NValue tacks on to that allocation.
    // Not even the intervening StringRef class has that level of detail.
    // The allocator uses this knowledge so it can back-stop NValue's
    // guards against over-large allocations.
    // The back-stop strategy for when NValue is unexpectedly not doing
    // its job is to throw a fatal exception.
    // An alternative to this arcane knowledge would be for the allocator
    // to simply define its own reasonable limits -- greater than or equal to
    // NValue's limits -- rather than exactly matching the NValue logic.
    // The actual allocation sizes are slightly larger than the lengths
    // being validated and rounded up here because two levels of allocator
    // logic add their overhead to the length values returned here.
    // That currently adds 12 bytes of overhead, so actual allocations range
    // from 14 bytes up to (1 MB + 16 bytes).
    static const int32_t NVALUE_LONG_OBJECT_LENGTHLENGTH = 4;
    if (length <= 2) {
        return 2;
    } else if (length <= 4) {
        return 4;
    } else if (length <= 4 + 2) {
        return 4 + 2;
    } else if (length <= 8) {
        return 8;
    } else if (length <= 8 + 4) {
        return 8 + 4;
    } else if (length <= 16) {
        return 16;
    } else if (length <= 16 + 8) {
        return 16 + 8;
    } else if (length <= 32) {
        return 32;
    } else if (length <= 32 + 16) {
        return 32 + 16;
    } else if (length <= 64) {
        return 64;
    } else if (length <= 64 + 32) {
        return 64 + 32;
    } else if (length <= 128) {
        return 128;
    } else if (length < 128 + 64) {
        return 128 + 64;
    } else if (length <= 256) {
        return 256;
    } else if (length <= 256 + 128) {
        return 256 + 128;
    } else if (length <= 512) {
        return 512;
    } else if (length <= 512 + 256) {
        return 512 + 256;
    } else if (length <= 1024) {
        return 1024;
    } else if (length <= 1024 + 512) {
        return 1024 + 512;
    } else if (length <= 2048) {
        return 2048;
    } else if (length <= 2048 + 1024) {
        return 2048 + 1024;
    } else if (length <= 4096) {
        return 4096;
    } else if (length <= 4096 + 2048) {
        return 4096 + 2048;
    } else if (length <= 8192) {
        return 8192;
    } else if (length <= 8192 + 4096) {
        return 8192 + 4096;
    } else if (length <= 16384) {
        return 16384;
    } else if (length <= 16384 + 8192) {
        return 16384 + 8192;
    } else if (length <= 32768) {
        return 32768;
    } else if (length <= 32768 + 16384) {
        return 32768 + 16384;
    } else if (length <= 65536) {
        return 65536;
    } else if (length <= 65536 + 32768) {
        return 65536 + 32768;
    } else if (length <= 131072) {
        return 131072;
    } else if (length <= 131072 + 65536) {
        return 131072 + 65536;
    } else if (length <= 262144) {
        return 262144;
    } else if (length <= 262144 + 131072) {
        return 262144 + 131072;
    } else if (length <= 524288) {
        return 524288;
    } else if (length <= 524288 + 262144) {
        return 524288 + 262144;
        // Need to allow space for a maximum-length string
        // WITH its 4-byte length prefix provided by NValue.
    } else if (length <= ThreadLocalPool::POOLED_MAX_VALUE_LENGTH +
               NVALUE_LONG_OBJECT_LENGTHLENGTH) {
        return ThreadLocalPool::POOLED_MAX_VALUE_LENGTH +
                NVALUE_LONG_OBJECT_LENGTHLENGTH;
    }
    throwFatalException("Attempted to allocate an object larger than the 1 MB limit. Requested size was %d",
                        length);
}

int TestOnlyAllocationSizeForObject(int length)
{
    return getAllocationSizeForObject(length);
}


#ifdef MEMCHECK
/// Persistent string pools with their compaction are completely bypassed for
/// the memcheck build. It just does standard C++ heap allocations and
/// deallocations.
char* ThreadLocalPool::allocateRelocatable(char** referrer_ignored, int32_t sz)
{
    char* data = new char[sz];
    return data;
}

// We COULD go through the trouble of allocating a Sized block and returning
// its "data" part in the MEMCHECK version of allocateRelocatable, just like
// in non-MEMCHECK mode. Then we could use its m_size here to give the exact
// same value as non-MEMCHECK mode, but for now, we are just letting the C++
// heap track the allocation size internally. That means we can only return
// an arbitrary guess here, hopefully good enough for MEMCHECK mode?
// Another alternative would be to rely on the "object length" that NValue
// encodes into the first 1-3 bytes of the data, but that breaks all kinds
// of abstraction and pollutes this code with higher-level NValue details.
// So, for now, use an arbitrary size that corresponds roughly for no really
// good reason to the minimum possible non-MEMCHECK string allocation.
int32_t ThreadLocalPool::getAllocationSizeForRelocatable(char* data_ignored)
{
    static const int32_t MEMCHECK_NOMINAL_ALLOCATION_SIZE =
        getAllocationSizeForObject(1) + static_cast<int32_t>(sizeof(int32_t)) +
        CompactingPool::FIXED_OVERHEAD_PER_ENTRY();
    return MEMCHECK_NOMINAL_ALLOCATION_SIZE;
}

void ThreadLocalPool::freeRelocatable(char* data)
{ delete [] data; }

#else // not MEMCHECK

static CompactingStringStorage& getStringPoolMap()
{
    return *static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
}

/// The layout of an allocation segregated by size,
/// including overhead to help identify the size-specific
/// pool from which the allocation must be freed.
/// The layout contains the size associated with its specific pool.
/// This size is stored in a header that is completely invisible
/// to the caller.
/// The caller is exposed only to the remaining "data" part of the
/// allocation, of at least their requested size.
/// So, the address of this data part is the proper return value for
/// allocate... and the proper argument to free....
/// Methods are provided to "convert" back and forth between the
/// pointer to the raw internal allocation and the data pointer
/// exposed to the caller.
struct Sized {
    int32_t m_size;
    char m_data[0];

    static Sized* fromAllocation(void* allocation, int32_t alloc_size)
    {
        Sized* result = reinterpret_cast<Sized*>(allocation);
        // Store the size so that it can be used
        // to find the right pool for deallocation.
        result->m_size = alloc_size;
        return result;
    }

    static Sized* backtrackFromCallerData(char* data)
    {
        Sized* result = reinterpret_cast<Sized*>(data - sizeof(Sized));
        // The data addresses should line up perfectly.
        assert(data == result->m_data);
        return result;
    }

};

char* ThreadLocalPool::allocateRelocatable(char** referrer, int32_t sz)
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
    int32_t alloc_size = getAllocationSizeForObject(sz) +
            static_cast<int32_t>(sizeof(Sized));
    CompactingStringStorage& poolMap = getStringPoolMap();
    CompactingStringStorage::iterator iter = poolMap.find(alloc_size);
    void* allocation;
    if (iter == poolMap.end()) {
        // There is no pool yet for objects of this size, so create one.
        // Compute num_elements to be the largest multiple of alloc_size
        // to fit in a 2MB buffer.
        int32_t num_elements = ((2 * 1024 * 1024 - 1) /
                (alloc_size + CompactingPool::FIXED_OVERHEAD_PER_ENTRY())) + 1;
        boost::shared_ptr<CompactingPool> pool(new CompactingPool(alloc_size, num_elements));
        poolMap.insert(std::pair<int32_t, boost::shared_ptr<CompactingPool> >(alloc_size, pool));
        allocation = pool->malloc(referrer);
    }
    else {
        allocation = iter->second->malloc(referrer);
    }

    // Convert from the raw allocation to the initialized size header and the
    // exposed caller data area.
    Sized* sized = Sized::fromAllocation(allocation, alloc_size);
    return sized->m_data;
}

int32_t ThreadLocalPool::getAllocationSizeForRelocatable(char* data)
{
    // Convert from the caller data to the size-prefixed allocation to
    // extract its size field.
    Sized* sized = Sized::backtrackFromCallerData(data);
    return sized->m_size + CompactingPool::FIXED_OVERHEAD_PER_ENTRY();
}

void ThreadLocalPool::freeRelocatable(char* data)
{
    // Convert from the caller data to the size-prefixed allocation
    // to return the correct raw allocation address to the correct pool.
    Sized* sized = Sized::backtrackFromCallerData(data);
    // use the cached size to find the right pool.
    int32_t alloc_size = sized->m_size;
    CompactingStringStorage& poolMap = getStringPoolMap();
    CompactingStringStorage::iterator iter = poolMap.find(alloc_size);
    if (iter == poolMap.end()) {
        // If the pool can not be found, there could not have been a prior
        // allocation for any object of this size, so either the caller
        // passed a bogus data pointer that was never allocated here OR
        // the data pointer's size header has been corrupted.
        throwFatalException("Attempted to free an object of an unrecognized size. Requested size was %d",
                            alloc_size);
    }
    // Free the raw allocation from the found pool.
    iter->second->free(sized);
}

#endif

void* ThreadLocalPool::allocateExactSizedObject(std::size_t sz)
{
    PoolsByObjectSize& pools =
            *(static_cast< PairTypePtr >(pthread_getspecific(m_key))->second);
    PoolsByObjectSize::iterator iter = pools.find(sz);
    PoolForObjectSize* pool;
    if (iter == pools.end()) {
        pool = new PoolForObjectSize(sz);
        PoolForObjectSizePtr poolPtr(pool);
        pools.insert(std::pair<std::size_t, PoolForObjectSizePtr>(sz, poolPtr));
    }
    else {
        pool = iter->second.get();
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
    return pool->malloc();
}

void ThreadLocalPool::freeExactSizedObject(std::size_t sz, void* object)
{
    PoolsByObjectSize& pools =
            *(static_cast< PairTypePtr >(pthread_getspecific(m_key))->second);
    PoolsByObjectSize::iterator iter = pools.find(sz);
    if (iter == pools.end()) {
        throwFatalException(
                "Failed to locate an allocated object of size %ld to free it.",
                static_cast<long>(sz));
    }
    PoolForObjectSize* pool = iter->second.get();
    pool->free(object);
}

std::size_t ThreadLocalPool::getPoolAllocationSize() {
    size_t bytes_allocated =
        *static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated));
    // For relocatable objects, each object-size-specific pool
    // -- or actually, its ContiguousAllocator -- tracks its own memory
    // allocation, so sum them, here.
    CompactingStringStorage* poolMap =
        static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
    for (CompactingStringStorage::iterator iter = poolMap->begin();
         iter != poolMap->end();
         ++iter) {
        bytes_allocated += iter->second->getBytesAllocated();
    }
    return bytes_allocated;
}

char * voltdb_pool_allocator_new_delete::malloc(const size_type bytes) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) += bytes + sizeof(std::size_t);
    //std::cout << "Pooled memory is " << ((*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) / (1024 * 1024)) << " after requested allocation " << (bytes / (1024 * 1024)) <<  std::endl;
    char *retval = new (std::nothrow) char[bytes + sizeof(std::size_t)];
    *reinterpret_cast<std::size_t*>(retval) = bytes + sizeof(std::size_t);
    return &retval[sizeof(std::size_t)];
}

void voltdb_pool_allocator_new_delete::free(char * const block) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) -= *reinterpret_cast<std::size_t*>(block - sizeof(std::size_t));
    delete [](block - sizeof(std::size_t));
}
}
