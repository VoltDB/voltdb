/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
        dst[len] = 0;
    }
protected:
    static unsigned char* query(size_t state, unsigned char* dst) {
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
    virtual unsigned char* query(size_t state) {
        return query(state, m_queryBuf);
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
    inline unsigned char* fill(unsigned char* dst) {
        memcpy(dst, get(), sizeof m_queryBuf);
        dst[len] = 0;
        return dst;
    }
    inline void* fill(void* dst) {
        return fill(reinterpret_cast<unsigned char*>(dst));
    }
    inline static unsigned char* of(unsigned char* dst, size_t state) {
        memcpy(dst, query(state), len + 1);
        dst[len] = 0;
        return dst;
    }
    inline static bool same(void const* dst, size_t state) {
        static unsigned char buf[len+1];
        return ! memcmp(dst, query(state, buf), len);
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
        static unsigned char buf[len+1];
        return hex(query(state, buf));
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

TEST_F(TableTupleAllocatorTest, HelloWorld) {
    // Test on StringGen test util
    /*
    StringGen<16> gen;
    for(auto c = 0; c < 500; ++c) {
        cout<<c<<": "<<StringGen<16>::hex(gen.get());
    }
    */
    // Test on LRU src util
    voltdb::LRU<10, int, int> lru;
    for(int i = 0; i < 10; ++i) {
        ASSERT_FALSE(lru.get(i));
        lru.add(i, i);
        ASSERT_EQ(*lru.get(i), i);
    }
    for(int i = 10; i < 20; ++i) {
        ASSERT_FALSE(lru.get(i));
        ASSERT_TRUE(lru.get(i - 10));
        lru.add(i, i);
        ASSERT_EQ(*lru.get(i), i);
        ASSERT_FALSE(lru.get(i - 10));
    }
    for(int i = 10; i < 20; ++i) {
        ASSERT_EQ(*lru.get(i), i);
    }
}

constexpr size_t TupleSize = 16;       // bytes per allocation
constexpr size_t AllocsPerChunk = 512 / TupleSize;     // 512 comes from ChunkHolder::chunkSize()
constexpr size_t NumTuples = 256 * AllocsPerChunk;     // # allocations: fits in 256 chunks

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
        auto const* iter = alloc.find(addresses[i]);
        ASSERT_NE(iter, nullptr);
        ASSERT_TRUE((*iter)->contains(addresses[i]));
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
//    assert(alloc_cref.size() == NumTuples);
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
        return ! memcmp(dst, mask(super::query(state, buf), state), TupleSize);
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

template<typename Chunks, size_t NthBit>
void testCustomizedIteratorCB() {
    using masked_const_iterator = typename
        IterableTableTupleChunks<Chunks, truth>::template iterator_cb_type<iterator_permission_type::ro>;
    using masked_iterator = typename
        IterableTableTupleChunks<Chunks, truth>::template iterator_cb_type<iterator_permission_type::rw>;
    using const_iterator = typename IterableTableTupleChunks<Chunks, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, truth>::iterator;
    using Gen = StringGen<TupleSize>;
    using Tag = NthBitChecker<NthBit>;
    Gen gen;
    Chunks alloc(TupleSize);
    auto const& alloc_cref = alloc;
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }

    static Tag const tag{};
    class Masker {                          // masks NthBit
        unsigned char m_buf[TupleSize];
    public:
        static void* set(void* p) {         // overwrites
            Tag::set(p);
            assert(tag(p));
            return p;
        }
        void const* operator()(void const* p) {  // provides a different view
            memcpy(m_buf, p, TupleSize);
            Tag::set(m_buf);
            return m_buf;
        }
    } masker;
    // test const_iterator on different view
    fold<masked_const_iterator>(                               // different view
            alloc_cref,
            [](void const* p) { assert(tag(p)); },
            masker);
    i = 0;
    fold<const_iterator>(alloc_cref, [&addresses, &i](void const* p) {
            assert(Gen::same(addresses[i], i));                // with original content untouched
            ++i;
        });
    assert(i == NumTuples);
    // test iterator that overwrites
    for_each<masked_iterator>(alloc, [](void* p) { assert(tag(p)); },
            [&masker](void*p) { return masker.set(p); });      // using functor needs &masker = as_const(masker) in capture
    fold<const_iterator>(alloc_cref, [](void const* p) { assert(tag(p)); });
}

/**
 * iterator that could either change its content (i.e. non-const
 * iterator), or that could provide a masked version of content
 * without changing its content (i.e. const iterator)
 */
TEST_F(TableTupleAllocatorTest, TestIteratorCB) {
    testCustomizedIteratorCB<NonCompactingChunks<EagerNonCompactingChunk>, 0>();
    testCustomizedIteratorCB<NonCompactingChunks<LazyNonCompactingChunk>, 1>();
    testCustomizedIteratorCB<CompactingChunks, 2>();
    testCustomizedIteratorCB<CompactingChunks, 3>();
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
                mask(super::query(state, buf)),
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
    auto const& alloc_cref = alloc;
    Hook hook(TupleSize, alloc.boundary());
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
    constexpr auto const InsertTuples = 256lu;                   // # tuples to be inserted/appended since snapshot

    array<void*, NumTuples + InsertTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {                     // the later appended ones will later be marked as inserted after starting snapshot
        addresses[i] = gen.fill(alloc.allocate());
    }
    hook.freeze();                                       // recording started
    alloc.freeze();                                      // don't forget to notify allocator, too
    // mark last 256 insertions as inserted after snapshot
    // started
    for (i = NumTuples; i < NumTuples + InsertTuples; ++i) {
        auto* p = alloc.allocate();
        hook.add(alloc, Hook::ChangeType::Insertion, p);
        addresses[i] = gen.fill(p);
        InsertionTag::set(p);                                  // mark as "insertion pending"
    }
    // test that the two bits we chose do not step over Gen's work, and txn iterator sees latest change
    i = 0;
    fold<const_iterator>(alloc_cref,
            [&insertionTag, &deletionUpdateTag, &i, &gen](void const* p) {
                assert((i >= NumTuples) == insertionTag(p));
                assert(! deletionUpdateTag(p));
                assert(gen.same(p, i));
                ++i;
            });
    assert(i == NumTuples + InsertTuples);
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
                    assert(hook.reverted(p) == p);
                    ++i;
                }
            }, hook);
    assert(i == NumTuples);                                    // snapshot does not see newly inserted rows
    for(; i < NumTuples + InsertTuples; ++i) {
        assert(hook.reverted(addresses[i]) == nullptr);
        assert(insertionTag(addresses[i]));
        InsertionTag::set(addresses[i]);
    }

    auto const DeletedTuples = 256lu,                          // Deleting 256th, 257th, ..., 511th allocations
         DeletedOffset = 256lu;
    // marks first 256 as deleted, and delete them. Notice how we
    // need to intertwine hook into the deletion process.

    for (i = DeletedOffset; i < DeletedOffset + DeletedTuples; ++i) {
        auto* src = addresses[i];
        hook.copy(src);                 // NOTE: client need to remember to call this, before making any deletes
        auto* dst = alloc.free(src);
        assert(dst);
        hook.add(alloc, Hook::ChangeType::Deletion, src);
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
    assert(i == NumTuples + InsertTuples - DeletedTuples);     // but transparent to txn

    auto const UpdatedTuples = 1024lu, UpdatedOffset = 1024lu;  // Updating 1024 <= 1025, 1025 <= 1026, ..., 2047 <= 2048
    // do the math: after we deleted first 256 entries, the
    // 2014th is what 2014 + 256 = 2260th entry once were.
    for (i = UpdatedOffset + DeletedTuples;
            i < UpdatedOffset + DeletedTuples + UpdatedTuples;
            ++i) {
        // for update changes, hook does not need to copy
        // tuple getting updated (i.e. dst), since the hook is
        // called pre-update.
        hook.add(alloc, Hook::ChangeType::Update, addresses[i]);    // 1280 == first eval
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
    assert(i == NumTuples + InsertTuples - DeletedTuples);
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
    TestTxnHook tst;
    tst();
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
    auto iterp = alloc.template freeze<truth>();
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
        alloc.update(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), addresses[i + 2000], TupleSize);
    }
    verify_snapshot_const();

    // Step 2: deletion
    for (i = 100; i < 900; ++i) {
        alloc.remove(const_cast<void*>(addresses[i]));
    }
    verify_snapshot_const();

    // Step 3: batch deletion, using single API
    set<void*> ss;
    for (i = 909; i < 999; ++i) {
        ss.emplace(const_cast<void*>(addresses[i]));
    }
    alloc.remove(ss, [](map<void*, void*> const& m) {});
    verify_snapshot_const();

    // Step 4: insertion
    for (i = 0; i < 500; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 5: update
    for (i = 2000; i < 2200; ++i) {
        alloc.update(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 6: deletion
    for (i = 3099; i < 3599; ++i) {
        alloc.remove(const_cast<void*>(addresses[i]));
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
                        alloc.remove(const_cast<void*>(latest[ii]));
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
                    alloc.update(const_cast<void*>(latest[ii]));
                    memcpy(const_cast<void*>(latest[ii]), gen.get(), TupleSize);
                    latest[ii] = p1;
                    p1 = nullptr;
                }
                break;
            case 3:                                            // batch remove, using separate APIs
                ii = 0;
                for_each<typename IterableTableTupleChunks<Alloc, truth>::iterator>(
                        alloc, [&alloc, &ii, &rgen, &whole](void* p) {
                            if (static_cast<double>(whole(rgen)) / NumTuples < 0.01) {     // 1% chance getting picked for batch deletion
                                ++ii;
                                alloc.remove_add(p);
                            }
                        });
                assert(alloc.remove_force() == ii);
            default:;
        }
        ++i;
    }
    verify_snapshot_const();
    // simulates actual snapshot process: memory clean up as we go
    iterp.reset(static_cast<typename decltype(iterp)::element_type*>(nullptr));// TODO: fix behavior on iterp
    i = 0;
//    while (! iterp->drained()) {                               // explicit use of snapshot RW iterator
//        void const* p = **iterp;
//        assert(Gen::same(p, i++));
//        alloc.release(p);                                      // snapshot of the tuple finished
//        ++*iterp;
//    }
    for_each<snapshot_iterator>(alloc, [&alloc, &i](void const* p) {
                assert(Gen::same(p, i++));
                alloc.release(p);                          // snapshot of the tuple finished
            });
    assert(i == NumTuples);
    alloc.thaw();
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
    set<void*> s;
    for (i = AllocsPerChunk - 10; i < AllocsPerChunk; ++i) {       // batch remove last 10 entries
        s.emplace(const_cast<void*>(addresses[i]));
    }
    alloc.remove(s, [](map<void*, void*> const&) {});
    verify_snapshot_const();
    alloc.thaw();
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
    set<void*> s;
    for (i = 0; i < 10; ++i) {       // batch remove last 10 entries
        s.emplace(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 10; ++i) {       // inserts another 10 different entries
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    alloc.remove(s, [](map<void*, void*> const&) {});
    verify_snapshot_const();
    alloc.thaw();
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
    alloc.remove(set<void*>{const_cast<void*>(addresses[4])},      // 9 => 4
            [](map<void*, void*> const&){});
    i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            alloc_cref,
            [&i](void const* p) {
                assert(Gen::same(p, i == 4 ? 9 : i));
                ++i;
            });
    assert(i == 9);
    alloc.thaw();
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

    set<void*> batch;
    auto iter = addresses.begin();
    for (i = 0; i < 3; ++i) {
        for_each(iter, next(iter, 10), [&batch](void const* p) { batch.emplace(const_cast<void*>(p)); });
        advance(iter, AllocsPerChunk - 10);
        for_each(iter, next(iter, 10), [&batch](void const* p) { batch.emplace(const_cast<void*>(p)); });
        advance(iter, 10);
    }
    assert(batch.size() == 60);
    alloc.remove(batch, [](map<void*, void*> const&){});
    verify_snapshot_const();
    alloc.thaw();
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
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(p == nullptr || Gen::same(p, i++));
                });
        assert(i == NumTuples);
    };
    alloc.template freeze<truth>();

    set<void*> batch;
    for(auto iter = addresses.cbegin();                             // remove every other
            iter != addresses.cend();) {
        batch.emplace(const_cast<void*>(*iter));
        if (++iter == addresses.cend()) {
            break;
        } else {
            ++iter;
        }
    }
    assert(batch.size() == NumTuples / 2);
    alloc.remove(batch, [](map<void*, void*> const&){});
    verify_snapshot_const();
    alloc.thaw();
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
    set<void*> batch;
    for(i = 0; i < AllocsPerChunk + 2; ++i) {                  // batch remove 1st chunk plus 2
        batch.emplace(const_cast<void*>(addresses[i]));
    }
    alloc.remove(batch, [](map<void*, void*> const&){});
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
    alloc.remove(const_cast<void*>(addresses[0]));             // single remove, twice
    alloc.remove(const_cast<void*>(addresses[1]));
    ASSERT_EQ(4, alloc.chunks());
    ASSERT_EQ(N - 2, alloc.size());
    // batch remove last 30 entries, compacts/removes head chunk
    set<void*> s;
    for(i = 2; i < AllocsPerChunk; ++i) {
        s.emplace(const_cast<void*>(addresses[N - i + 1]));
    }
    alloc.remove(s, [](map<void*, void*> const&){});
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
    TestHookedCompactingChunks t;
    t();
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
                        alloc.remove(const_cast<void*>(addresses[i1]));
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
                        alloc.update(const_cast<void*>(addresses[i2]));
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
    alloc.thaw();
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
    TestInterleavedCompactingChunks t;
    t();
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
    alloc.template freeze<truth>();                                     // single chunk, not full before freeze,
    alloc.remove(const_cast<void*>(addresses[0]));             // then a few deletions
    alloc.remove(const_cast<void*>(addresses[5]));
    alloc.remove(const_cast<void*>(addresses[10]));
    alloc.remove(const_cast<void*>(addresses[20]));
    using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
    i = 0;
    for_each<snapshot_iterator>(alloc, [&alloc, &i](void const* p) {
        if (p != nullptr) {
            assert(Gen::same(p, i++));
            alloc.release(p);
        }
    });
    assert(i == Number);
    alloc.thaw();
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
    TestSingleChunkSnapshot t;
    t();
}

#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}

