/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "common/debuglog.h"
#include "storage/TableTupleAllocator.hpp"
#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <random>
#include <thread>
#include <chrono>

using TableTupleAllocatorTest = Test;
// These tests are geared towards debug build, relying on some
// constants defined differently in the src file.
#ifndef NDEBUG

using namespace voltdb::storage;
using namespace std;

static random_device rd;
/**
 * For usage, see commented test in HelloWorld
 */
template<size_t len> class StringGen {
    unsigned char m_queryBuf[len + 1];
    size_t m_state = 0;
    static void reset(unsigned char* dst) {
        memset(dst, 1, len);
    }
protected:
    virtual unsigned char* query(size_t state) {
        return of(state, m_queryBuf);
    }
public:
    inline explicit StringGen() {
        reset(m_queryBuf);
    }
    inline void reset() {
        m_state = 0;
        reset(m_queryBuf);
    }
    inline unsigned char* get() {
        return query(m_state++);
    }
    inline size_t last_state() const noexcept {
        return m_state;
    }
    inline unsigned char* fill(unsigned char* dst) {
        memcpy(dst, get(), len);
        return dst;
    }
    inline void* fill(void* dst) {
        return fill(reinterpret_cast<unsigned char*>(dst));
    }
    static unsigned char* of(size_t state, unsigned char* dst) {       // ENC
        reset(dst);
        for(size_t s = state, pos = 0; s && pos <= len; ++pos) {
            size_t s1 = s, acc = 1;
            while(s1 > 255) {
                s1 /= 255;
                acc *= 255;
            }
            dst[pos] = s % 255 + 1;
            s /= 255;
        }
        return dst;
    }
    inline static size_t of(unsigned char const* dst) {      // DEC
        size_t r = 0, i = 0, j = 0;
        // rfind 1st position > 1
        for (j = len - 1; j > 0 && dst[j] == 1; --j) {}
        for (i = 0; i <= j; ++i) {
            r = r * 255 + dst[j - i] - 1;
        }
        return r;
    }
    inline static bool same(void const* dst, size_t state) {
        static unsigned char buf[len+1];
        return ! memcmp(dst, of(state, buf), len);
    }
    static string hex(unsigned char const* src) {
        static char n[7];
        const int perLine = 16;
        string r;
        r.reserve(len * 5.2);
        for(size_t pos = 0; pos < len; ++pos) {
            snprintf(n, sizeof n, "0x%x ", src[pos]);
            r.append(n);
            if (pos % perLine == perLine - 1) {
                r.append("\n");
            }
        }
        return r.append("\n");
    }
    inline static string hex(void const* src) {
        return hex(reinterpret_cast<unsigned char const*>(src));
    }
    inline static string hex(size_t state) {
        static unsigned char buf[len];
        return hex(of(state, buf));
    }
};

TEST_F(TableTupleAllocatorTest, RollingNumberComparison) {
#define RollingNumberComparisons(type)                                            \
    ASSERT_TRUE(less_rolling(numeric_limits<type>::max(),                         \
                static_cast<type>(numeric_limits<type>::max() + 1)));             \
    ASSERT_FALSE(less_rolling(static_cast<type>(numeric_limits<type>::max() + 1), \
                numeric_limits<type>::max()))
    RollingNumberComparisons(unsigned char);
    RollingNumberComparisons(unsigned short);
    RollingNumberComparisons(unsigned);
    RollingNumberComparisons(unsigned long);
    RollingNumberComparisons(unsigned long long);
    RollingNumberComparisons(size_t);
#undef RollingNumberComparisons
}

constexpr size_t TupleSize = 16;       // bytes per allocation
constexpr size_t AllocsPerChunk = 512 / TupleSize;     // 512 comes from ChunkHolder::chunkSize()
constexpr size_t NumTuples = 256 * AllocsPerChunk;     // # allocations: fits in 256 chunks

TEST_F(TableTupleAllocatorTest, TestStringGen_static) {
    using Gen = StringGen<TupleSize>;
    unsigned char buf[TupleSize];
    for (auto i = 0lu; i < NumTuples * 10; ++i) {
        ASSERT_EQ(i, Gen::of(Gen::of(i, buf)));
    }
}

template<typename Chunks>
void testNonCompactingChunks(size_t outOfOrder) {
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;

    assert(alloc.empty());
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }

    // Test non-sequential free() calls
    for(size_t i = 0; i < outOfOrder; ++i) {
        for(size_t j = 0; j < NumTuples; ++j) {
            if (j % outOfOrder == i) {
                assert(Gen::same(addresses[j], j));
                alloc.free(addresses[j]);
                // non-compacting chunks don't compact upon free()'s
                // Myth: so long as I don't release the last of
                // every #outOfOrder-th allocation, the chunk
                // itself holds.
                assert(i + 1 == outOfOrder || Gen::same(addresses[j], j));
            }
        }
    }
    assert(alloc.empty());                 // everything gone
}

TEST_F(TableTupleAllocatorTest, TestChunkListFind) {
    CompactingChunks alloc(TupleSize);
    array<void*, 3 * AllocsPerChunk> addresses;
    for(auto i = 0; i < addresses.size(); ++i) {
        addresses[i] = alloc.allocate();
    }
    for(auto i = 0; i < addresses.size(); ++i) {
        auto const iter = alloc.find(addresses[i]);
        ASSERT_TRUE(iter.first);
        ASSERT_TRUE(iter.second->contains(addresses[i]));
    }
}

template<typename Chunks>
void testIteratorOfNonCompactingChunks() {
    using const_iterator = typename IterableTableTupleChunks<Chunks, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, truth>::iterator;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void*, NumTuples>;
    Gen gen;
    Chunks alloc(TupleSize);
    size_t i;
    addresses_type addresses;

    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    assert(alloc.size() == NumTuples);
    class Checker {
        size_t m_index = 0;                // Note that NonCompactingChunks uses singly-linked list
        map<void const*, size_t> m_remains;
    public:
        Checker(addresses_type const& addr) {
            for(size_t i = 0; i < tuple_size<addresses_type>::value; ++i) {
                m_remains.emplace(addr[i], i);
            }
        }
        void operator() (void const* p) {
            assert(m_remains.count(p));
            assert(Gen::same(p, m_remains[p]));
            m_remains.erase(p);
        }
        void operator() (void* p) {
            assert(m_remains.count(p));
            assert(Gen::same(p, m_remains[p]));
            m_remains.erase(p);
        }
        bool complete() const noexcept {
            return m_remains.empty();
        }
    };

    Checker c2{addresses};
    for_each<iterator>(alloc, c2);
    assert(c2.complete());

    // free in FIFO order: using functor
    class Remover {
        size_t m_i = 0;
        Chunks& m_chunks;
    public:
        Remover(Chunks& c): m_chunks(c) {}
        void operator()(void* p) {
            m_chunks.free(p);
            ++m_i;
        }
        size_t invocations() const {
            return m_i;
        }
    } remover{alloc};
    for_each<iterator>(alloc, remover);
    assert(remover.invocations() == NumTuples);
    assert(alloc.empty());

    // free in FIFO order: using lambda
    // First, re-allocate everything
    for(i = 0; i < NumTuples; ++i) {
        gen.fill(alloc.allocate());
    }
    assert(! alloc.empty());
    assert(alloc.size() == NumTuples);
    i = 0;
    for_each<iterator>(alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });
    assert(i == NumTuples);
    assert(alloc.empty());
    // Iterating on empty chunks is a no-op
    for_each<iterator>(alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });
    assert(i == NumTuples);
    // expected compiler error: cannot mix const_iterator with non-const
    // lambda function. Uncomment next 2 lines to see
    /*for_each<typename IterableTableTupleChunks<Chunks, truth>::const_iterator>(
            alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });*/
}

TEST_F(TableTupleAllocatorTest, TestNonCompactingChunks) {
    for (size_t outOfOrder = 5; outOfOrder < 10; ++outOfOrder) {
        testNonCompactingChunks<NonCompactingChunks<EagerNonCompactingChunk>>(outOfOrder);
        testNonCompactingChunks<NonCompactingChunks<LazyNonCompactingChunk>>(outOfOrder);
    }
}

TEST_F(TableTupleAllocatorTest, TestIteratorOfNonCompactingChunks) {
    testIteratorOfNonCompactingChunks<NonCompactingChunks<EagerNonCompactingChunk>>();
    testIteratorOfNonCompactingChunks<NonCompactingChunks<LazyNonCompactingChunk>>();
}

void testCompactingChunks() {
    using Gen = StringGen<TupleSize>;
    using const_iterator = typename IterableTableTupleChunks<CompactingChunks, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<CompactingChunks, truth>::iterator;
    Gen gen;
    CompactingChunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(Gen::same(addresses[i], i));
    }
    i = 0;
    auto const& alloc_cref = alloc;
    // allocation memory order is consistent with iterator order
    fold<const_iterator>(alloc_cref,
            [&i, &addresses](void const* p) { assert(p == addresses[i++]); });
    assert(i == NumTuples);
    assert(alloc_cref.size() == NumTuples);
    // testing compacting behavior
    // 1. free() call sequence that does not trigger compaction
    assert(CompactingChunks::Compact::value);
    // free() from either end: freed values should not be
    // overwritten.
    // shrink from head is a little twisted:
    for(int j = 0; j < NumTuples / AllocsPerChunk; ++j) {      // on each chunk, free from its tail
        for(int i = AllocsPerChunk - 1; i >= 0; --i) {
            auto index = j * AllocsPerChunk + i;
            if (addresses[index] != alloc.free(addresses[index])) {
                assert(false);
            }
            // skip content check on i == 0, since OS has already claimed the chunk upon free() call
            assert(i == 0 || Gen::same(addresses[index], index));
        }
    }
    assert(alloc.empty());
    // 2. always trigger compaction, by free()-ing in the opposite order
    gen.reset();
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(Gen::same(addresses[i], i));
    }
    size_t j = 0;
    // shrink from head: free in LIFO order triggers compaction in "roughly" opposite direction
    // 1st half: chop from tail, replaced by "head" (the tail of the first chunk, to be exact)
    for(i = 0; i < NumTuples / 2; ++i, ++j) {
        auto const chunkful = i / AllocsPerChunk * AllocsPerChunk,
             indexInsideChunk = AllocsPerChunk - 1 - (i % AllocsPerChunk);
        if (addresses[chunkful + indexInsideChunk] != alloc.free(addresses[NumTuples - 1 - i])) {
            assert(false);
        }
    }
    // 2nd half
    i = 0;
    fold<const_iterator>(alloc_cref, [&i, &addresses](void const* p)
            { addresses[i++] = const_cast<void*>(p); });
    for(i = 0; i < NumTuples / 4; ++i, ++j) {
        auto const chunkful = i / AllocsPerChunk * AllocsPerChunk,
             indexInsideChunk = AllocsPerChunk - 1 - (i % AllocsPerChunk);
        if (addresses[chunkful + indexInsideChunk] != alloc.free(addresses[NumTuples / 2 - 1 - i])) {
            assert(false);
        }
    }
    // free them all! See note on IterableTableTupleChunks on
    // why we need a loop of calls to iterate through.
    while(! alloc.empty()) {
        for_each<iterator>(alloc, [&alloc, &j](void* p) {      // no-ops on boundary of each compacted chunk
                if (alloc.free(p) != nullptr) {++j;} });
    }
    assert(j == NumTuples);            // number of free() calls
}

template<typename Alloc, typename Compactible = typename Alloc::Compact> struct TrackedDeleter {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed |= m_alloc.free(p) != nullptr;
    }
};
// non-compacting chunks' free() is void. Not getting tested, see
// comments at caller.
template<typename Alloc> struct TrackedDeleter<Alloc, integral_constant<bool, false>> {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed = true;
        m_alloc.free(p);
    }
};

template<typename Tag>
class MaskedStringGen : public StringGen<TupleSize> {
    using super = StringGen<TupleSize>;
    size_t const m_skipped;
    unsigned char* mask(unsigned char* p, size_t state) const {
        if (state % m_skipped) {
            Tag::set(p);
        } else {
            Tag::reset(p);
        }
        return p;
    }
public:
    explicit MaskedStringGen(size_t skipped): m_skipped(skipped) {}
    unsigned char* query(size_t state) override {
        return mask(super::query(state), state);
    }
    inline bool same(void const* dst, size_t state) {
        static unsigned char buf[TupleSize+1];
        return ! memcmp(dst, mask(super::of(state, buf), state), TupleSize);
    }
};

template<typename Chunks, size_t NthBit>
void testCustomizedIterator(size_t skipped) {      // iterator that skips on every #skipped# elems
    using Tag = NthBitChecker<NthBit>;
    using const_iterator = typename IterableTableTupleChunks<Chunks, Tag>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, Tag>::iterator;
    MaskedStringGen<Tag> gen(skipped);

    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(gen.same(addresses[i], i));
    }
    i = 0;
    auto const& alloc_cref = alloc;
    fold<const_iterator>(alloc_cref, [&i, &addresses, skipped](void const* p) {
                if (i % skipped == 0) {
                    ++i;
                }
                if (Chunks::Compact::value) {
                    assert(p == addresses[i]);
                }
                ++i;
            });
    assert(i == NumTuples);
    // Free all tuples via allocator. Note that allocator needs
    // to be aware the "emptiness" from the iterator POV, not
    // allocator's POV.

    // Note: see document on NonCompactingChunks to understand
    // why we cannot use iterator to free memories.
    Tag const tag;
    if (Chunks::Compact::value) {
        bool freed;
        TrackedDeleter<Chunks> deleter(alloc, freed);
        do {
            freed = false;
            try {
                for_each<iterator>(alloc, [&deleter, &tag](void* p) {
                        assert(tag(p));
                        deleter(p);
                    });
            } catch (range_error const&) {}
        } while(freed);
        // Check using normal iterator (i.e. non-skipping one) that
        // the remains are what should be.
        fold<typename IterableTableTupleChunks<Chunks, truth>::const_iterator>(
                alloc_cref, [&tag](void const* p) { assert(! tag(p)); });
    } else {       // to free on compacting chunk safely, collect and free separately
        forward_list<void const*> crematorium;
        i = 0;
        fold<const_iterator>(alloc_cref, [&crematorium, &tag, &i](void const* p) {
                assert(tag(p));
                crematorium.emplace_front(p);
                ++i;
            });
        for_each(crematorium.begin(), crematorium.end(),
                [&alloc](void const*p) { alloc.free(const_cast<void*>(p)); });
        // We should not check/free() the rest using
        // iterator, for much the same reason that we don't use
        // same iterator-delete pattern (i.e. for_each) as
        // compactible allocators.
    }
}

TEST_F(TableTupleAllocatorTest, TestCompactingChunks) {
    testCompactingChunks();
    for (auto skipped = 8lu; skipped < 64; skipped += 8) {
        testCustomizedIterator<CompactingChunks, 3>(skipped);
        testCustomizedIterator<NonCompactingChunks<EagerNonCompactingChunk>, 3>(skipped);
        testCustomizedIterator<NonCompactingChunks<LazyNonCompactingChunk>, 3>(skipped);
        testCustomizedIterator<CompactingChunks, 6>(skipped);       // a different mask
    }
}

// expression template used to apply variadic NthBitChecker
template<typename Tuple, size_t N> struct Apply {
    using Tag = typename tuple_element<N, Tuple>::type;
    Apply<Tuple, N-1> const m_next{};
    inline unsigned char* operator()(unsigned char* p) const {
        Tag::reset(p);
        return m_next(p);
    }
};
template<typename Tuple> struct Apply<Tuple, 0> {
    using Tag = typename tuple_element<0, Tuple>::type;
    inline unsigned char* operator()(unsigned char* p) const {
        Tag::reset(p);
        return p;
    }
};

template<typename TagsAsTuple>
class UnmaskedStringGen : public StringGen<TupleSize> {
    using super = StringGen<TupleSize>;
    static Apply<TagsAsTuple, tuple_size<TagsAsTuple>::value - 1> const applicator;
    static unsigned char* mask(unsigned char* p) {
        return applicator(p);
    }
    static unsigned char* copy_mask(void const* p) {
        static unsigned char copy[TupleSize];
        memcpy(copy, p, TupleSize);
        return applicator(copy);
    }
public:
    unsigned char* query(size_t state) override {
        return mask(super::query(state));
    }
    inline bool same(void const* dst, size_t state) {
        static unsigned char buf[TupleSize+1];
        return ! memcmp(
                copy_mask(dst),
                mask(super::of(state, buf)),
                TupleSize);
    }
    inline static string hex(void const* p) {
        return super::hex(copy_mask(p));
    }
    inline string hex(size_t s) {
        return super::hex(copy_mask(query(s)));
    }
};
template<typename TagsAsTuple> Apply<TagsAsTuple, tuple_size<TagsAsTuple>::value - 1> const UnmaskedStringGen<TagsAsTuple>::applicator{};

/**
 * In this test, the Chunks is ignorant to whether there is
 * a snapshot or not, and we are using TxnHook together with
 * hook-aware iterator, and the abstraction is still quite
 * leaky...
 *
 * Ideally, we should let CompactingChunks know when the snapshot
 * started/stopped, and client does not need to know the hook a
 * lot (i.e. let the Chunks notify the hook when it
 * started/stopped).
 */
template<typename HookAlloc, typename DataAlloc, typename RetainTrait>
void testTxnHook() {
    using const_iterator = typename IterableTableTupleChunks<DataAlloc, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<DataAlloc, truth>::iterator;
    using Hook = TxnPreHook<HookAlloc, RetainTrait>;
    DataAlloc alloc(TupleSize);
    using dummy_observer_type = typename IterableTableTupleChunks<HookedCompactingChunks<Hook>, truth>::IteratorObserver;
    auto const& alloc_cref = alloc;
    Hook hook(TupleSize);
    /**
     * We reserve 2 bits in the leading char to signify that this
     * tuple is newly inserted, thus invisible in snapshot view (bit#7),
     * and is deleted or updated (bit#6), thus snapshot view should retrieve
     * the actual thing from *the hook*.
     */
    using InsertionTag = NthBitChecker<7>;
    using DeletionUpdateTag = NthBitChecker<0>;
    InsertionTag const insertionTag{};
    DeletionUpdateTag const deletionUpdateTag{};

    using Gen = UnmaskedStringGen<tuple<InsertionTag, DeletionUpdateTag>>;
    Gen gen{};

    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {                     // the later appended ones will later be marked as inserted after starting snapshot
        addresses[i] = gen.fill(alloc.allocate());
    }
    hook.freeze();                                       // recording started
    alloc.freeze();                                      // don't forget to notify allocator, too
    // test that the two bits we chose do not step over Gen's work, and txn iterator sees latest change
    i = 0;
    fold<const_iterator>(alloc_cref,
            [&insertionTag, &deletionUpdateTag, &i, &gen](void const* p) {
                assert((i >= NumTuples) == insertionTag(p));
                assert(! deletionUpdateTag(p));
                assert(gen.same(p, i));
                ++i;
            });
    assert(i == NumTuples);
    using snapshot_iterator = typename
        IterableTableTupleChunks<DataAlloc, truth>::template iterator_cb<TxnPreHook<HookAlloc, RetainTrait>>;
    using const_snapshot_iterator = typename
        IterableTableTupleChunks<DataAlloc, truth>::template const_iterator_cb<TxnPreHook<HookAlloc, RetainTrait>>;
    i = 0;
    fold<const_snapshot_iterator>(alloc_cref,
            [&insertionTag, &deletionUpdateTag, &hook, &i, &gen](void const* p) {
                if (p != nullptr) {                            // see document on iterator_cb_type for why client needs to do the checking
                    assert(! insertionTag(p));
                    assert(! deletionUpdateTag(p));
                    assert(gen.same(p, i));
                    assert(hook(p) == p);
                    ++i;
                }
            }, hook);
    assert(i == NumTuples);                                    // snapshot does not see newly inserted rows

    auto const DeletedTuples = 256lu,                          // Deleting 256th, 257th, ..., 511th allocations
         DeletedOffset = 256lu;
    // marks first 256 as deleted, and delete them. Notice how we
    // need to intertwine hook into the deletion process.

    for (i = DeletedOffset; i < DeletedOffset + DeletedTuples; ++i) {
        auto* src = addresses[i];
        hook.copy(src);                 // NOTE: client need to remember to call this, before making any deletes
        auto* dst = alloc.free(src);
        assert(dst);
        hook._add_for_test_(Hook::ChangeType::Deletion, src);
        DeletionUpdateTag::reset(dst);   // NOTE: sequencing this before the hook would cause std::stack<void*> default ctor to crash in GLIBC++7 (~L94 of TableTupleAllocator.h), in /usr/include/c++/7/ext/new_allocator.h
    }
    i = 0;
    fold<const_snapshot_iterator>(alloc_cref,
            [&insertionTag, &i, &gen](void const* p) {
                if (p != nullptr) {
                    assert(! insertionTag(p));
                    assert(gen.same(p, i));                    // snapshot sees no delete changes
                    ++i;
                }
            }, hook);
    assert(i == NumTuples);                                    // opaque to snapshot
    i = 0;
    fold<const_iterator>(alloc_cref, [&i](void const*) { ++i; });
    assert(i == NumTuples - DeletedTuples);     // but transparent to txn

    auto const UpdatedTuples = 1024lu, UpdatedOffset = 1024lu;  // Updating 1024 <= 1025, 1025 <= 1026, ..., 2047 <= 2048
    // do the math: after we deleted first 256 entries, the
    // 2014th is what 2014 + 256 = 2260th entry once were.
    for (i = UpdatedOffset + DeletedTuples;
            i < UpdatedOffset + DeletedTuples + UpdatedTuples;
            ++i) {
        // for update changes, hook does not need to copy
        // tuple getting updated (i.e. dst), since the hook is
        // called pre-update.
        hook._add_for_test_(Hook::ChangeType::Update, addresses[i]);    // 1280 == first eval
        memcpy(addresses[i], addresses[i + 1], TupleSize);
        DeletionUpdateTag::reset(addresses[i]);
    }
    i = 0;
    fold<const_snapshot_iterator>(alloc_cref,
            [&insertionTag, &i, &gen](void const* p) {
                if (p != nullptr) {
                    assert(! insertionTag(p));
                    assert(gen.same(p, i));                    // snapshot sees no update changes
                    ++i;
                }
            }, hook);
    assert(i == NumTuples);
    i = 0;
    fold<const_iterator>(alloc_cref, [&i](void const*) { ++i; });
    assert(i == NumTuples - DeletedTuples);
    // Hook releasal should be done as we cover tuples in
    // snapshot process. We delay the release here to help
    // checking invariant on snapshot view.
    for_each<snapshot_iterator>(alloc,
            [&hook](void const* p) {
                hook.release(p);
            }, hook);
    // Verify that we cannot create two snapshot iterators at the
    // same time
    if (DataAlloc::Compact::value) {
        using snapshot_rw_iterator =
            typename IterableTableTupleChunks<DataAlloc, truth>::
            template iterator_type<iterator_permission_type::rw, iterator_view_type::snapshot>;
        {                                                      // verifiy on base iterator type
            snapshot_rw_iterator iter1(alloc);
            try {
                snapshot_rw_iterator iter2(alloc);             // expected to throw
                assert(false);
            } catch (logic_error const& e) {
                assert(string(e.what()).substr(0, 52) ==
                        "Cannot create RW snapshot iterator on chunk list id ");
            }
        }
        {                                                      // verifiy on iterator_cb_type
            auto iter1 = snapshot_iterator::begin(alloc, hook);
            try {
                auto iter2 = snapshot_iterator::begin(alloc, hook);
                assert(false);
            } catch (logic_error const& e) {
                assert(string(e.what()).substr(0, 52) ==
                        "Cannot create RW snapshot iterator on chunk list id ");
            }
        }
        // But it's okay to create multiple snapshot RO iterators
        auto iter1 = const_snapshot_iterator::begin(alloc, hook),
             iter2 = const_snapshot_iterator::begin(alloc, hook);
        // or RW iterators on different allocators
        DataAlloc alloc2(TupleSize);
        auto iter10 = snapshot_iterator::begin(alloc, hook),
             iter20 = snapshot_iterator::begin(alloc2, hook);
    }
    hook.thaw();
    alloc.thaw();
}

template<typename Alloc1> struct TestTxnHook1 {
    void operator()() const {
        testTxnHook<Alloc1, CompactingChunks, HistoryRetainTrait<gc_policy::never>>();
        testTxnHook<Alloc1, CompactingChunks, HistoryRetainTrait<gc_policy::always>>();
        testTxnHook<Alloc1, CompactingChunks, HistoryRetainTrait<gc_policy::batched>>();
    }
};
struct TestTxnHook {
    static TestTxnHook1<NonCompactingChunks<EagerNonCompactingChunk>> const sChain1;
    static TestTxnHook1<NonCompactingChunks<LazyNonCompactingChunk>> const sChain2;
    void operator()() const {
        sChain1();
        sChain2();
    }
};
TestTxnHook1<NonCompactingChunks<EagerNonCompactingChunk>> const TestTxnHook::sChain1{};
TestTxnHook1<NonCompactingChunks<LazyNonCompactingChunk>> const TestTxnHook::sChain2{};

TEST_F(TableTupleAllocatorTest, TestTxnHook) {
    TestTxnHook()();
}

/**
 * Test of HookedCompactingChunks using its RW iterator that
 * effects (GC) as snapshot process continues.
 */
template<typename Chunk, gc_policy pol>
void testHookedCompactingChunks() {
    using Hook = TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    auto iterp = alloc.template freeze<truth>();               // check later that snapshot iterator persists over > 10,000 txns
    using const_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_iterator;
    using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
    auto const verify_snapshot_const = [&alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(Gen::same(p, i++));
        });
        assert(i == NumTuples);
    };
    i = 0;
    fold<const_iterator>(alloc_cref, [&i](void const* p) { assert(Gen::same(p, i++)); });
    assert(i == NumTuples);
    // Operations during snapshot. The indexes used in each step
    // are absolute.
    // 1. Update: record 200-1200 <= 2200-3200
    // 2. Delete: record 100 - 900
    // 3. Batch Delete: record 910 - 999
    // 4. Insert: 500 records
    // 5. Update: 2000 - 2200 <= 0 - 200
    // 6. Delete: 3099 - 3599
    // 7. Randomized 5,000 operations

    // Step 1: update
    for (i = 200; i < 1200; ++i) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), addresses[i + 2000], TupleSize);
    }
    verify_snapshot_const();

    // Step 2: deletion
    for (i = 100; i < 900; ++i) {
        alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[i]));
    }
    verify_snapshot_const();

    // Step 3: batch deletion
    alloc.remove_reserve(90);
    for (i = 909; i < 999; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
            for_each(entries.begin(), entries.end(),
                    [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    verify_snapshot_const();

    // Step 4: insertion
    for (i = 0; i < 500; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 5: update
    for (i = 2000; i < 2200; ++i) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 6: deletion
    for (i = 3099; i < 3599; ++i) {
        alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[i]));
    }
    verify_snapshot_const();

    // Step 7: randomized operations
    vector<void const*> latest;
    latest.reserve(NumTuples);
    fold<const_iterator>(alloc_cref, [&latest](void const* p) { latest.emplace_back(p); });
    // FIXME: a known bug when you replace rd() next line to seed 63558933, change upper bound on i to 4935,
    // and set a break point on case 2 when i == 4934. The
    // verifier errornouly reads out extra artificial data beyond
    // 8192 values.
    mt19937 rgen(rd());
    uniform_int_distribution<size_t> range(0, latest.size() - 1), changeTypes(0, 3), whole(0, NumTuples - 1);
    void const* p1 = nullptr;
    for (i = 0; i < 8000;) {
        size_t ii;
        vector<void*> tb_removed;
        switch(changeTypes(rgen)) {
            case 0:                                            // insertion
                p1 = memcpy(alloc.allocate(), gen.get(), TupleSize);
                break;
            case 1:                                            // deletion
                ii = range(rgen);
                if (latest[ii] == nullptr) {
                    continue;
                } else {
                    try {
                        alloc.template _remove_for_test_<truth>(const_cast<void*>(latest[ii]));
                        latest[ii] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {             // if we tried to delete a non-existent address, pick another option
                        continue;
                    }
                    break;
                }
            case 2:                                            // update
                ii = range(rgen);
                if (latest[ii] == nullptr) {
                    continue;
                } else {
                    alloc.template update<truth>(const_cast<void*>(latest[ii]));
                    memcpy(const_cast<void*>(latest[ii]), gen.get(), TupleSize);
                    latest[ii] = p1;
                    p1 = nullptr;
                }
                break;
            case 3:                                            // batch remove, using separate APIs
                tb_removed.clear();
                fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                        static_cast<Alloc const&>(alloc), [&tb_removed, &rgen, &whole](void const* p) {
                            if (static_cast<double>(whole(rgen)) / NumTuples < 0.01) {     // 1% chance getting picked for batch deletion
                                tb_removed.emplace_back(const_cast<void*>(p));
                            }
                        });
                alloc.remove_reserve(tb_removed.size());
                ii = 0;
                for_each(tb_removed.cbegin(), tb_removed.cend(),
                        [&ii, &alloc](void* p) {
                            ++ii;
                            auto const& entry = alloc.template remove_add<truth>(p);
                            switch (entry.status_of()) {
                                case Hook::added_entry_t::status::existing:
                                case Hook::added_entry_t::status::ignored:
                                    break;
                                case Hook::added_entry_t::status::fresh:
                                default:
                                    assert(! memcmp(const_cast<typename Hook::added_entry_t&>(entry).copy_of(),
                                        p, TupleSize));
                            }
                        });
                assert(alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                            for_each(entries.begin(), entries.end(),
                                    [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                            }) == tb_removed.size());
            default:;
        }
        ++i;
    }
    verify_snapshot_const();
    // simulates actual snapshot process: memory clean up as we go
    i = 0;
    while (! iterp->drained()) {                               // explicit use of snapshot RW iterator
        void const* p = **iterp;
        assert(Gen::same(p, i++));
        alloc.release(p);                                      // snapshot of the tuple finished
        ++*iterp;
    }
    assert(i == NumTuples);
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single1() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    auto const verify_snapshot_const = [&alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
    };
    alloc.remove_reserve(10);
    for (i = AllocsPerChunk - 10; i < AllocsPerChunk; ++i) {       // batch remove last 10 entries
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
            for_each(entries.begin(), entries.end(),
                    [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single2() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc, &alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
    };
    alloc.remove_reserve(10);
    for (i = 0; i < 10; ++i) {       // batch remove last 10 entries
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 10; ++i) {       // inserts another 10 different entries
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
            for_each(entries.begin(), entries.end(),
                    [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single3() {         // correctness on txn view: single elem remove
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, 10>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < 10; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    alloc.remove_reserve(1);
    alloc.template remove_add<truth>(const_cast<void*>(addresses[4]));      // 9 => 4
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            alloc_cref,
            [&i](void const* p) {
                assert(Gen::same(p, i == 4 ? 9 : i));
                ++i;
            });
    assert(i == 9);
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single4() {         // correctness on txn view: single elem table, remove the only row
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    Alloc alloc(TupleSize);
    void* p = alloc.allocate();
    alloc.remove_reserve(1);
    alloc.template remove_add<truth>(p);
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    assert(alloc.empty());
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_multi1() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk * 3>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk * 3; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc]() {
        auto const& alloc_cref = alloc;
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk * 3);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
                });
        assert(i == AllocsPerChunk * 3);
    };
    alloc.template freeze<truth>();

    auto iter = addresses.begin();
    alloc.remove_reserve(60);
    for (i = 0; i < 3; ++i) {
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
                alloc.template remove_add<truth>(const_cast<void*>(p));
            });
        advance(iter, AllocsPerChunk - 10);
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
                alloc.template remove_add<truth>(const_cast<void*>(p));
            });
        advance(iter, 10);
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_multi2() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc]() {
        auto const& alloc_cref = alloc;
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
            });
        assert(i == NumTuples);
    };
    alloc.template freeze<truth>();

    alloc.remove_reserve(NumTuples / 2);
    for(auto iter = addresses.cbegin();                             // remove every other
            iter != addresses.cend();) {
        alloc.template remove_add<truth>(const_cast<void*>(*iter));
        if (++iter == addresses.cend()) {
            break;
        } else {
            ++iter;
        }
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

TEST_F(TableTupleAllocatorTest, testHookedCompactingChunksBatchRemove_nonfull_2chunks) {
    using HookAlloc = NonCompactingChunks<LazyNonCompactingChunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<gc_policy::never>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk * 2 - 2>;     // 2 chunks, 2nd 2 allocs from full
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < addresses.size(); ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.remove_reserve(AllocsPerChunk + 2);
    for(i = 0; i < AllocsPerChunk + 2; ++i) {                  // batch remove 1st chunk plus 2
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    ASSERT_EQ(AllocsPerChunk - 4, alloc.size());
}

TEST_F(TableTupleAllocatorTest, testHookedCompactingChunksStatistics) {
    using HookAlloc = NonCompactingChunks<LazyNonCompactingChunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<gc_policy::never>>;
    using Alloc = HookedCompactingChunks<Hook>;
    auto constexpr N = AllocsPerChunk * 3 + 2;
    array<void const*, N> addresses;
    Alloc alloc(TupleSize);
    ASSERT_EQ(TupleSize, alloc.tupleSize());
    ASSERT_EQ(0, alloc.chunks());
    ASSERT_EQ(0, alloc.size());
    size_t i;
    for(i = 0; i < N; ++i) {
        addresses[i] = alloc.allocate();
    }
    ASSERT_EQ(4, alloc.chunks());
    ASSERT_EQ(N, alloc.size());
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[0]));             // single remove, twice
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[1]));
    ASSERT_EQ(4, alloc.chunks());
    ASSERT_EQ(N - 2, alloc.size());
    // batch remove last 30 entries, compacts/removes head chunk
    alloc.remove_reserve(AllocsPerChunk - 2);
    for(i = 2; i < AllocsPerChunk; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[N - i + 1]));
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept {
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    ASSERT_EQ(3, alloc.chunks());
    ASSERT_EQ(N - AllocsPerChunk, alloc.size());
}

template<typename Chunk, gc_policy pol> struct TestHookedCompactingChunks2 {
    inline void operator()() const {
        testHookedCompactingChunks<Chunk, pol>();
        // batch removal tests assume head-compacting direction
        testHookedCompactingChunksBatchRemove_single1<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single2<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single3<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single4<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_multi1<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_multi2<Chunk, pol>();
    }
};
template<typename Chunk> struct TestHookedCompactingChunks1 {
    static TestHookedCompactingChunks2<Chunk, gc_policy::never> const s1;
    static TestHookedCompactingChunks2<Chunk, gc_policy::always> const s2;
    static TestHookedCompactingChunks2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::never>
const TestHookedCompactingChunks1<Chunk>::s1{};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::always>
const TestHookedCompactingChunks1<Chunk>::s2{};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::batched>
const TestHookedCompactingChunks1<Chunk>::s3{};

struct TestHookedCompactingChunks {
    static TestHookedCompactingChunks1<EagerNonCompactingChunk> const s1;
    static TestHookedCompactingChunks1<LazyNonCompactingChunk> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
TestHookedCompactingChunks1<EagerNonCompactingChunk> const TestHookedCompactingChunks::s1{};
TestHookedCompactingChunks1<LazyNonCompactingChunk> const TestHookedCompactingChunks::s2{};

TEST_F(TableTupleAllocatorTest, TestHookedCompactingChunks) {
    TestHookedCompactingChunks()();
}

/**
 * Simulates how MP execution works: interleaved snapshot
 * advancement with txn in progress.
 */
template<typename Chunk, gc_policy pol>
void testInterleavedCompactingChunks() {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    using const_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_iterator;
    using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
    using explicit_iterator_type = pair<size_t, snapshot_iterator>;
    explicit_iterator_type explicit_iter(0, snapshot_iterator::begin(alloc));
    auto verify = [] (explicit_iterator_type& iter) {
        assert(*iter.second == nullptr || Gen::same(*iter.second, iter.first));
    };
    auto advance_verify = [&verify](explicit_iterator_type& iter) {     // returns false on draining
        if (iter.second.drained()) {
            return false;
        }
        verify(iter);
        ++iter.first;
        ++iter.second;
        if (! iter.second.drained()) {
            verify(iter);
            return true;
        } else {
            return false;
        }
    };
    auto advances_verify = [&advance_verify] (explicit_iterator_type& iter, size_t advances) {
        for (size_t i = 0; i < advances; ++i) {
            if (! advance_verify(iter)) {
                return false;
            }
        }
        return true;
    };

    mt19937 rgen(rd());
    uniform_int_distribution<size_t> range(0, NumTuples - 1), changeTypes(0, 2),
        advanceTimes(0, 4);
    void const* p1 = nullptr;
    for (i = 0; i < 8000;) {
        size_t i1, i2;
        switch(changeTypes(rgen)) {
            case 0:                                            // insertion
                memcpy(const_cast<void*&>(p1) = alloc.allocate(), gen.get(), TupleSize);
                break;
            case 1:                                            // deletion
                i1 = range(rgen);
                if (addresses[i1] == nullptr) {
                    continue;
                } else {
                    try {
                        alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[i1]));
                        addresses[i1] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {                 // if we tried to delete a non-existent address, pick another option
                        continue;
                    }
                    break;
                }
            case 2:                                            // update
            default:;
                    i1 = range(rgen); i2 = range(rgen);
                    if (i1 == i2 || addresses[i1] == nullptr || addresses[i2] == nullptr) {
                        continue;
                    } else {
                        alloc.template update<truth>(const_cast<void*>(addresses[i2]));
                        memcpy(const_cast<void*>(addresses[i2]), addresses[i1], TupleSize);
                        addresses[i2] = p1;
                        p1 = nullptr;
                    }
        }
        ++i;
        if (! advances_verify(explicit_iter, advanceTimes(rgen))) {
            break;                                             // verified all snapshot iterators
        }
    }
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol> struct TestInterleavedCompactingChunks2 {
    inline void operator()() const {
        for (size_t i = 0; i < 16; ++i) {                      // repeat randomized test
            testInterleavedCompactingChunks<Chunk, pol>();
        }
    }
};
template<typename Chunk> struct TestInterleavedCompactingChunks1 {
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::never> const s1;
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::always> const s2;
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::never>
const TestInterleavedCompactingChunks1<Chunk>::s1{};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::always>
const TestInterleavedCompactingChunks1<Chunk>::s2{};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::batched>
const TestInterleavedCompactingChunks1<Chunk>::s3{};

struct TestInterleavedCompactingChunks {
    static TestInterleavedCompactingChunks1<EagerNonCompactingChunk> const s1;
    static TestInterleavedCompactingChunks1<LazyNonCompactingChunk> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
TestInterleavedCompactingChunks1<EagerNonCompactingChunk> const TestInterleavedCompactingChunks::s1{};
TestInterleavedCompactingChunks1<LazyNonCompactingChunk> const TestInterleavedCompactingChunks::s2{};

TEST_F(TableTupleAllocatorTest, TestInterleavedOperations) {
    TestInterleavedCompactingChunks()();
}

template<typename Chunk, gc_policy pol>
void testSingleChunkSnapshot() {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>>;
    using Gen = StringGen<TupleSize>;
    static constexpr auto Number = AllocsPerChunk - 3;
    using addresses_type = array<void const*, Number>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < Number; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();                            // single chunk, not full before freeze,
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[0]));             // then a few deletions
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[5]));
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[10]));
    alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[20]));
    i = 0;
    for_each<typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator>(
            alloc, [&alloc, &i](void const* p) {
                assert(Gen::same(p, i++));
                alloc.release(p);
            });
    assert(i == Number);
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol> struct TestSingleChunkSnapshot2 {
    inline void operator()() const {
        testSingleChunkSnapshot<Chunk, pol>();
    }
};
template<typename Chunk> struct TestSingleChunkSnapshot1 {
    static TestSingleChunkSnapshot2<Chunk, gc_policy::never> const s1;
    static TestSingleChunkSnapshot2<Chunk, gc_policy::always> const s2;
    static TestSingleChunkSnapshot2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk>TestSingleChunkSnapshot2<Chunk, gc_policy::never>
const TestSingleChunkSnapshot1<Chunk>::s1{};
template<typename Chunk>TestSingleChunkSnapshot2<Chunk, gc_policy::always>
const TestSingleChunkSnapshot1<Chunk>::s2{};
template<typename Chunk>TestSingleChunkSnapshot2<Chunk, gc_policy::batched>
const TestSingleChunkSnapshot1<Chunk>::s3{};
struct TestSingleChunkSnapshot {
    static TestSingleChunkSnapshot1<EagerNonCompactingChunk> const s1;
    static TestSingleChunkSnapshot1<LazyNonCompactingChunk> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
TestSingleChunkSnapshot1<EagerNonCompactingChunk> const TestSingleChunkSnapshot::s1{};
TestSingleChunkSnapshot1<LazyNonCompactingChunk> const TestSingleChunkSnapshot::s2{};

TEST_F(TableTupleAllocatorTest, TestSingleChunkSnapshot) {
    TestSingleChunkSnapshot()();
}

template<typename Chunk, gc_policy pol, CompactingChunks::remove_direction dir>
void testRemovesFromEnds(size_t batch) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    Gen gen;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        memcpy(const_cast<void*>(addresses[i] = alloc.allocate()), gen.get(), TupleSize);
    }
    assert(alloc.size() == NumTuples);
    // remove tests
    if (dir == CompactingChunks::remove_direction::from_head) {      // remove from head
        for (i = 0; i < batch; ++i) {
            alloc.remove(dir, addresses[i]);
        }
        alloc.remove(dir, nullptr);     // completion
        assert(alloc.size() == NumTuples - batch);
        fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [&i](void const* p) { assert(Gen::same(p, i++)); });
        assert(i == NumTuples);
        alloc.template freeze<truth>();
        try {                                                  // not allowed when frozen
            alloc.remove(dir, nullptr);
            assert(false);                                     // should have failed
        } catch (logic_error const& e) {
            assert(! strcmp(e.what(),
                        "HookedCompactingChunks::remove(dir, ptr): Cannot remove from head when frozen"));
            alloc.template thaw<truth>();
        }
    } else {                                                   // remove from tail
        for (i = NumTuples - 1; i >= NumTuples - batch && i < NumTuples; --i) {
            alloc.remove(dir, addresses[i]);
        }
        assert(alloc.size() == NumTuples - batch);
        i = 0;
        until<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [batch, &i](void const* p) {
                    assert(Gen::same(p, i));
                    return ++i >= NumTuples - batch;
                });
        assert(i == NumTuples - batch);
        // remove everything, add something back
        if (! alloc.empty()) {
            for (--i; i > 0; --i) {
                alloc.remove(dir, addresses[i]);
            }
            alloc.remove(dir, addresses[0]);
            assert(alloc.empty());
        }
        for (i = 0; i < NumTuples; ++i) {
            memcpy(alloc.allocate(), gen.get(), TupleSize);
        }
        fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [&i](void const* p) {
                    assert(Gen::same(p, i++));
                });
        assert(i == NumTuples * 2);
    }
}

template<typename Chunk, gc_policy pol, CompactingChunks::remove_direction dir>
struct TestRemovesFromEnds3 {
    inline void operator()() const {
        testRemovesFromEnds<Chunk, pol, dir>(0);
        testRemovesFromEnds<Chunk, pol, dir>(NumTuples);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk - 1);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk + 1);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk * 15 + 2);
    }
};

template<typename Chunk, gc_policy pol> struct TestRemovesFromEnds2 {
    static TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_head> const s1;
    static TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_tail> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
template<typename Chunk, gc_policy pol>
TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_head> const TestRemovesFromEnds2<Chunk, pol>::s1{};
template<typename Chunk, gc_policy pol>
TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_tail> const TestRemovesFromEnds2<Chunk, pol>::s2{};

template<typename Chunk> struct TestRemovesFromEnds1 {
    static TestRemovesFromEnds2<Chunk, gc_policy::never> const s1;
    static TestRemovesFromEnds2<Chunk, gc_policy::always> const s2;
    static TestRemovesFromEnds2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::never> const TestRemovesFromEnds1<Chunk>::s1{};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::always> const TestRemovesFromEnds1<Chunk>::s2{};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::batched> const TestRemovesFromEnds1<Chunk>::s3{};

struct TestRemovesFromEnds {
    inline void operator()() const {
        TestRemovesFromEnds1<EagerNonCompactingChunk>()();
        TestRemovesFromEnds1<LazyNonCompactingChunk>()();
    }
};

TEST_F(TableTupleAllocatorTest, TestRemovesFromEnds) {
    TestRemovesFromEnds()();
}

TEST_F(TableTupleAllocatorTest, TestClearReallocate) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    void* addr = alloc.allocate();
    ASSERT_EQ(addr, alloc.template _remove_for_test_<truth>(addr));
    // empty: reallocate
    memcpy(addr = alloc.allocate(), gen.get(), TupleSize);
    size_t i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            static_cast<Alloc const&>(alloc), [addr, this, &i](void const* p) {
                ASSERT_EQ(addr, p);
                ASSERT_TRUE(Gen::same(p, i++));
            });
    ASSERT_EQ(1, i);
}

// Test that it should work without txn in progress
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic0) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::elastic_iterator>(
            static_cast<Alloc const&>(alloc), [&i, this](void const* p) {
                ASSERT_TRUE(Gen::same(p, i++));
            });
    ASSERT_EQ(NumTuples, i);
}

// Test that it should work with insertions
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic1) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    size_t i;
    for(i = 0; i < NumTuples/ 2; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    for (i = 0; i < NumTuples / 2; ++i) {                      // iterator advance, then insertion in a loop
        ASSERT_TRUE(Gen::same(*iter++, i));
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    while (! iter.drained()) {
        ASSERT_TRUE(Gen::same(*iter++, i++));
    }
    ASSERT_EQ(NumTuples / 2, i);                       // won't see any newly inserted values
}

// Test that it should work with normal, compacting removals that only eats what had
// been iterated
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic2) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    for (i = 0; i < NumTuples && ! iter.drained(); ++iter) {                      // iterator advance, then delete previous iterated tuple
        // expensive O(n) check (actually almost O(AllocsPerChunk))
        void const* pp = *iter;
        bool const matched = until<IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc), [pp] (void const* p) { return ! memcmp(pp, p, TupleSize); });
        ASSERT_TRUE(matched);
        try {
            alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[i++]));
        } catch (range_error const&) {                         // OK bc. compaction
            continue;
        }
    }
}

// Test that it should work with normal, compacting removals in
// opposite direction of the/any iterator
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic3) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    for (i = 0; i < (NumTuples - AllocsPerChunk) / 2 && ! iter.drained(); ++i, ++iter) {
        void const* pp = *iter;
        bool const matched = until<IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc), [pp] (void const* p) { return ! memcmp(pp, p, TupleSize); });
        ASSERT_TRUE(matched);
        alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[NumTuples - i - 1]));
    }
    while (! iter.drained()) {
        void const* pp = *iter;
        bool const matched = until<IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc), [pp] (void const* p) { return ! memcmp(pp, p, TupleSize); });
        ASSERT_TRUE(matched);
        ++iter;
        ++i;
    }
}

// Test that it should work with lightweight, non-compacting removals that only eats
// what had been iterated
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic4) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    for (i = 0; i < NumTuples; ++i) {
        ASSERT_TRUE(Gen::same(*iter++, i));
        // Removing from head, unless forced by calling again
        // with NULL next, would not have any effect except
        // removing crosses boundary. This means that we are
        // iterating over values that actually should *not* be
        // visible (i.e. garbage). But that is ok, since we are
        // not forcing it every time (in which case it becomes
        // normal, compacting removal).
        alloc.remove(CompactingChunks::remove_direction::from_head, addresses[i]);
    }
    ASSERT_TRUE(iter.drained());
}

// Test that it should work with lightweight, non-compacting removals from tail
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic5) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    for (i = 0; i < NumTuples && ! iter.drained(); ++i) {
        ASSERT_TRUE(Gen::same(*iter++, i));
        // Removing from head, unless forced by calling again
        // with NULL next, would not have any effect except
        // removing crosses boundary. This means that we are
        // iterating over values that actually should *not* be
        // visible (i.e. garbage). But that is ok, since we are
        // not forcing it every time (in which case it becomes
        // normal, compacting removal). TODO: memcheck
        alloc.remove(CompactingChunks::remove_direction::from_tail, addresses[NumTuples - i - 1]);
    }
    ASSERT_TRUE(iter.drained());
    ASSERT_EQ(NumTuples / 2, i);
}

// Test that it should work when iterator created when allocator
// is empty
TEST_F(TableTupleAllocatorTest, TestElasticIterator_basic6) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    auto iter = IterableTableTupleChunks<Alloc, truth>::elastic_iterator::begin(alloc);
    Gen gen;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 2; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    for (i = 0; i < AllocsPerChunk * 2; ++i) {
        ASSERT_TRUE(Gen::same(*iter++, i));
    }
    ASSERT_TRUE(iter.drained());
    ASSERT_EQ(AllocsPerChunk * 2, i);
}

TEST_F(TableTupleAllocatorTest, TestSnapshotIteratorOnNonFull1stChunk) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    // Remove last 10, making 1st chunk non-full
    alloc.remove_reserve(10);
    for (i = 0; i < 10; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[NumTuples - i - 1]));
    }
    alloc.remove_force([](vector<pair<void*, void*>> const& entries)noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
    alloc.template freeze<truth>();
    i = 0;
    auto const beg = addresses.begin(), end = prev(addresses.end(), 10);
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator>(
            static_cast<Alloc const&>(alloc), [this, &i, &beg, &end] (void const* p) {
                ASSERT_TRUE(end != find(beg, end, p) ||
                        end != find_if(beg, end, [p](void const* pp) { return ! memcmp(p, pp, TupleSize); }));
                ++i;
            });
    ASSERT_EQ(NumTuples - 10, i);
    alloc.template thaw<truth>();
}

/**
 * Test clear() on hooked compacting chunks in presence of frozen state
 */
TEST_F(TableTupleAllocatorTest, TestClearFrozenCompactingChunks) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    size_t i;
    for (i = 0; i < NumTuples - 6; ++i) {                                              // last chunk not full
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    alloc.template clear<truth>();
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            static_cast<Alloc const&>(alloc),
            [this] (void const*) { ASSERT_FALSE(true); });                             // txn should see nothing
    i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator>(      // snapshot should see everything
            static_cast<Alloc const&>(alloc),
            [&i, this](void const* p) { ASSERT_TRUE(Gen::same(p, i++)); });
    ASSERT_EQ(NumTuples - 6, i);
    alloc.template thaw<truth>();
    ASSERT_TRUE(alloc.empty());
    for (i = 0; i < 6; ++i) {                                                          // next, after wipe out, insert 6 tuples
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    i = NumTuples - 6;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(             // check re-inserted content
            static_cast<Alloc const&>(alloc),
            [&i, this] (void const* p) { ASSERT_TRUE(Gen::same(p, i++)); });
    ASSERT_EQ(NumTuples, i);
}

/**
 * Test clear() on hooked compacting chunks, in absence of frozen state
 */
TEST_F(TableTupleAllocatorTest, TestClearFreeCompactingChunks) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    Gen gen;
    size_t i;
    for (i = 0; i < NumTuples - 6; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    alloc.template clear<truth>();
    ASSERT_TRUE(alloc.empty());
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            static_cast<Alloc const&>(alloc),
            [this] (void const*) { ASSERT_FALSE(true); });
    for (i = 0; i < 6; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    i = NumTuples - 6;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            static_cast<Alloc const&>(alloc),
            [&i, this] (void const* p) { ASSERT_TRUE(Gen::same(p, i++)); });
    ASSERT_EQ(NumTuples, i);
}

string address(void const* p) {
    ostringstream oss;
    oss<<p;
    return oss.str();
}

// test printing of debug info
TEST_F(TableTupleAllocatorTest, TestDebugInfo) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Alloc alloc(TupleSize);
    array<void const*, NumTuples> addresses;
    Gen gen;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    ASSERT_TRUE(alloc.info(nullptr).substr(0, 20) == "Cannot find address ");
    string expected_prefix("Address "),
           actual = alloc.info(addresses[0]);
    expected_prefix
        .append(address(addresses[0]))
        .append(" found at chunk 0, offset 0, ");
    ASSERT_EQ(expected_prefix, actual.substr(0, expected_prefix.length()));
    // freeze, remove 1 + AllocsPerChunk tuples from head and
    // tail each
    alloc.template freeze<truth>();
    for (i = 0; i <= AllocsPerChunk; ++i) {
        alloc.template _remove_for_test_<truth>(const_cast<void*>(addresses[i]));
        alloc.remove(CompactingChunks::remove_direction::from_tail, addresses[NumTuples - i - 1]);
    }
    ASSERT_EQ(NumTuples - 2 * AllocsPerChunk - 2, alloc.size());
    expected_prefix = "Address ";
    expected_prefix.append(address(addresses[0]))
        .append(" found at chunk 0, offset 0, txn 1st chunk = 1 [")
        .append(address(addresses[AllocsPerChunk]))
        .append(" - ");
    ASSERT_EQ(expected_prefix, alloc.info(addresses[0]).substr(0, expected_prefix.length()));
}

class finalize_verifier {
    using Gen = StringGen<TupleSize>;
    size_t const m_total;
    unordered_set<size_t> m_states{};
public:
    finalize_verifier(size_t n) : m_total(n) {
        assert(m_total > 0);
        m_states.reserve(m_total);
    }
    void reset(size_t n) {
        assert(n > 0);
        const_cast<size_t&>(m_total) = n;
        m_states.clear();
        m_states.reserve(n);
    }
    void operator()(void const* p) {
        assert(m_states.emplace(Gen::of(reinterpret_cast<unsigned char const*>(p))).second);
    }
    unordered_set<size_t> const& seen() const {
        return m_states;
    }
    bool ok(size_t start) const {
        if (m_states.size() != m_total) {
            return false;
        } else {
            auto const bounds = minmax_element(m_states.cbegin(), m_states.cend());
            return *bounds.first == start &&
                *bounds.first + m_total - 1 == *bounds.second;
        }
    }
};

TEST_F(TableTupleAllocatorTest, TestFinalizer_AllocsOnly) {
    // test allocation-only case
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        gen.fill(alloc.allocate());
    }
    alloc.template clear<truth>();
    ASSERT_TRUE(verifier.ok(0));
    verifier.reset(NumTuples);
    for (i = 0; i < NumTuples; ++i) {
        gen.fill(alloc.allocate());
    }
    alloc.template clear<truth>();
    ASSERT_TRUE(verifier.ok(NumTuples));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_AllocAndRemoves) {
    // test batch removal without frozen
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // batch remove every other tuple
    alloc.remove_reserve(NumTuples / 2);
    for (i = 0; i < NumTuples; i += 2) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    ASSERT_EQ(NumTuples / 2,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept {
                    for_each(entries.begin(), entries.end(),
                            [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    ASSERT_EQ(NumTuples / 2, verifier.seen().size());
    for (i = 0; i < NumTuples; i += 2) {
        ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
    }
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_FrozenRemovals) {
    // test batch removal when frozen, then thaw.
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    alloc.template freeze<truth>();
    alloc.remove_reserve(NumTuples / 2);
    for (i = 0; i < NumTuples; i += 2) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    ASSERT_EQ(NumTuples / 2,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept {
                    for_each(entries.begin(), entries.end(),
                            [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    ASSERT_TRUE(verifier.seen().empty());
    alloc.template thaw<truth>();
    // At thaw time, those copies in the batch should be removed
    // (and finalized before being deallocated), since snapshot iterator needs them
    ASSERT_EQ(NumTuples / 2, verifier.seen().size());
    for (i = 0; i < NumTuples; i += 2) {
        ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
    }
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_AllocAndUpdates) {
    // test updates when frozen, then thaw
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples + NumTuples / 2};
    Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    alloc.template freeze<truth>();
    // update with newest states
    for (i = 0; i < NumTuples; i += 2) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        gen.fill(const_cast<void*>(addresses[i]));
    }
    ASSERT_TRUE(verifier.seen().empty());
    alloc.template thaw<truth>();
    ASSERT_EQ(NumTuples / 2, verifier.seen().size());
    for (i = 0; i < NumTuples; i += 2) {
        ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
    }
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_InterleavedIterator) {
    // test allocation-only case
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples + NumTuples / 2};
    Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
    array<void const*, NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    auto const& iter = alloc.template freeze<truth>();
    for (i = 0; i < NumTuples / 2; ++i) {
        ++*iter;    // advance snapshot iterator to half way
    }
    // update every other tuple; but only those in the 2nd hald
    // are kept track of in the hook, and thus, a quarter of them
    // finalized.
    for (i = 0; i < NumTuples; i += 2) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        gen.fill(const_cast<void*>(addresses[i]));
    }
    ASSERT_TRUE(verifier.seen().empty());                                  // copies for updates finalized at thaw, or release() time, whichever comes first
    // delete the second half; but only half of those deleted
    // batch are "fresh", so only 1/8 of the whole gets to be
    // finalized
    alloc.remove_reserve(NumTuples / 2);
    // but interleaved with advancing snapshot iterator to 3/4 of
    // the whole course
    for (i = 0; i < NumTuples / 4; ++i) {
        ++*iter;
    }
    for (i = 0; i < NumTuples / 2; ++i) {
        alloc.template remove_add<truth>(
                const_cast<void*>(addresses[NumTuples - i - 1]));
    }
    // finalize called on the 2nd half in the txn memory
    ASSERT_EQ(NumTuples / 2, alloc.remove_force(
                [](vector<pair<void*, void*>> const& entries){
                    for_each(entries.begin(), entries.end(),
                            [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    ASSERT_TRUE(verifier.seen().empty());
    alloc.template thaw<truth>();
    ASSERT_EQ(NumTuples / 2 - NumTuples / 8, verifier.seen().size());
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_SimpleDtor) {
    // test that dtor should properly finalize
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    {
        Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
        size_t i;
        for (i = 0; i < NumTuples; ++i) {
            gen.fill(alloc.allocate());
        }
    }
    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_Snapshot) {
    // test finalizer on iterator
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples + AllocsPerChunk * 3};            // 2 additional chunks inserted, one chunk updated
    {
        Alloc alloc(TupleSize, [&verifier](void const* p) { verifier(p); });
        array<void const*, NumTuples + AllocsPerChunk * 2> addresses;
        size_t i;
        for (i = 0; i < NumTuples; ++i) {
            addresses[i] = gen.fill(alloc.allocate());
        }
        auto const& iter = alloc.template freeze<truth>();
        // After frozen, make some new allocations (2 new chunk);
        // some updates (10th chunk)
        // and batch remove from head (delete first 3 chunks) and
        // 2nd to last chunk, totaling 4 chunks
        for (i = 0; i < AllocsPerChunk * 2; ++i) {     // accounts for 2 additinal chunks to be finalized
            addresses[i + NumTuples] = gen.fill(alloc.allocate());
        }
        for (i = AllocsPerChunk * 10; i < AllocsPerChunk * 11; ++i) {      // accounts for 1 chunk
            alloc.template update<truth>(const_cast<void*>(addresses[i]));
            ASSERT_EQ(NumTuples + AllocsPerChunk * 2 + i - AllocsPerChunk * 10,
                    Gen::of(reinterpret_cast<unsigned char*>(
                            gen.fill(const_cast<void*>(addresses[i])))));
        }
        alloc.remove_reserve(AllocsPerChunk * 4);
        for (i = 0; i < AllocsPerChunk * 3; ++i) {                         // does not account for any chunks
            alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
        }
        ASSERT_TRUE(verifier.seen().empty());
        for (i = 0; i < AllocsPerChunk; ++i) {
            // Deleting beyond frozen region should trigger
            // finalize right away (same as unfrozen state):
            alloc.template remove_add<truth>(const_cast<void*>(addresses[NumTuples + i]));
        }

        ASSERT_EQ(AllocsPerChunk, verifier.seen().size());
        // verify that this 2nd to last chunk is finalized on
        // each call to `remove_add'
        for (i = NumTuples; i < NumTuples + AllocsPerChunk; ++i) {
            ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
        }

        ASSERT_EQ(AllocsPerChunk * 4,
                alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept{
                    for_each(entries.begin(), entries.end(),
                            [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                    }));
        // check 1st value of txn iterator
        ASSERT_EQ(AllocsPerChunk * 4, Gen::of(reinterpret_cast<unsigned char const*>(
                        *IterableTableTupleChunks<Alloc, truth>::const_iterator(alloc))));
        // use iterator for first 4 chunks, before thawing
        for (i = 0; i < AllocsPerChunk * 4; ++i) {
            // See src document for why using snapshot iterator
            // on snapshot-visible-only chunks **should not**
            // trigger any finalization.
            ++*iter;
        }
        ASSERT_EQ(AllocsPerChunk, verifier.seen().size());
        alloc.template thaw<truth>();
        /**
         * verify what hook had finalized
         */
        ASSERT_EQ(AllocsPerChunk * 5, verifier.seen().size());
        // batch removal on first 3 chunks: no compaction
        for (i = 0; i < AllocsPerChunk * 3; ++i) {
            ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
        }
        // update of 10th chunk
        for (i = AllocsPerChunk * 10; i < AllocsPerChunk * 11; ++i) {
            ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
        }
        // batch removal on 2nd to last chunk: this chunk is
        // finalized as soon as remove_add is called (as verified
        // above)
        for (i = NumTuples; i < NumTuples + AllocsPerChunk; ++i) {
            ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
        }
    }
    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestSnapIterBug_rep1) {
    // test finalizer on iterator
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    Alloc alloc(TupleSize);
    using Gen = StringGen<TupleSize>;
    Gen gen;
    array<void*, AllocsPerChunk * 3> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 3 ; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // delete last 2 tuples from last chunk
    alloc.remove_reserve(2);
    alloc.template remove_add<truth>(
            const_cast<void*>(addresses[AllocsPerChunk * 3 - 1]));
    alloc.template remove_add<truth>(
            const_cast<void*>(addresses[AllocsPerChunk * 3 - 2]));
    ASSERT_EQ(2,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    // then, freeze, and remove 1 full chunk worth of tuples
    auto const& iter = alloc.template freeze<truth>();
    alloc.remove_reserve(AllocsPerChunk);
    for (i = 0; i < AllocsPerChunk - 2; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 2; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i + AllocsPerChunk]));
    }
    ASSERT_EQ(AllocsPerChunk,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    // verify that snapshot should not see values deleted in the
    // 1st batch
    i = 0;
    while (! iter->drained()) {
        auto const val = Gen::of(reinterpret_cast<unsigned char*>(**iter));
        // should not see deleted values before freeze;
        ASSERT_NE(AllocsPerChunk * 3 - 1, val);
        ASSERT_NE(AllocsPerChunk * 3 - 2, val);
        ++(*iter);
        ++i;
    }
    ASSERT_EQ(AllocsPerChunk * 3 - 2, i);
    alloc.template thaw<truth>();
}

TEST_F(TableTupleAllocatorTest, TestSnapIterBug_rep2) {
    // test finalizer on iterator
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    Alloc alloc(TupleSize);
    using Gen = StringGen<TupleSize>;
    Gen gen;
    array<void*, AllocsPerChunk * 3> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 3 ; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // delete last 2 tuples from 1st chunk
    alloc.remove_reserve(2);
    alloc.template remove_add<truth>(
            const_cast<void*>(addresses[AllocsPerChunk - 1]));
    alloc.template remove_add<truth>(
            const_cast<void*>(addresses[AllocsPerChunk - 2]));
    ASSERT_EQ(2,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    // then, freeze, and remove 1 full chunk worth of tuples
    auto const& iter = alloc.template freeze<truth>();
    alloc.remove_reserve(AllocsPerChunk);
    for (i = 0; i < AllocsPerChunk - 2; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 2; ++i) {
        alloc.template remove_add<truth>(const_cast<void*>(addresses[i + AllocsPerChunk]));
    }
    ASSERT_EQ(AllocsPerChunk,
            alloc.remove_force([](vector<pair<void*, void*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }));
    // verify that snapshot should not see values deleted in the
    // 1st batch
    i = 0;
    while (! iter->drained()) {
        auto const val = Gen::of(reinterpret_cast<unsigned char*>(**iter));
        // should not see holes created due to deletion before freeze;
        ASSERT_NE(AllocsPerChunk - 1, val);
        ASSERT_NE(AllocsPerChunk - 2, val);
        ++(*iter);
        ++i;
    }
    ASSERT_EQ(AllocsPerChunk * 3 - 2, i);
    alloc.template thaw<truth>();
}


#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}

