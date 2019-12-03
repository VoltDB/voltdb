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
#include <cstdio>
#include <cstring>
#include <iostream>

using namespace voltdb::storage;
using namespace std;

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

struct TableTupleAllocatorTest : public Test {
};

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

    auto const& alloc_cref = alloc;
    Checker c1{addresses};
    for_each<const_iterator>(alloc_cref, c1);
    assert(c1.complete());

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

template<typename Chunks>
void testCompactingChunks() {
    using Gen = StringGen<TupleSize>;
    using const_iterator = typename IterableTableTupleChunks<Chunks, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, truth>::iterator;
    Gen gen;
    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(Gen::same(addresses[i], i));
    }
    i = 0;
    auto const& alloc_cref = alloc;
    // allocation memory order is consistent with iterator order
    for_each<const_iterator>(alloc_cref,
            [&i, &addresses](void const* p) { assert(p == addresses[i++]); });
    assert(i == NumTuples);
    // testing compacting behavior
    // 1. free() call sequence that does not trigger compaction
    bool const shrinkFromHead = Chunks::DIRECTION == ShrinkDirection::head;
    // free() from either end: freed values should not be
    // overwritten.
    if (shrinkFromHead) {   // shrink from head is a little twisted:
        for(int j = 0; j < NumTuples / AllocsPerChunk; ++j) {      // on each chunk, free from its tail
            for(int i = AllocsPerChunk - 1; i >= 0; --i) {
                auto index = j * AllocsPerChunk + i;
                assert(addresses[index] == alloc.free(addresses[index]));
                // skip content check on i == 0, since OS has already claimed the chunk upon free() call
                assert(i == 0 || Gen::same(addresses[index], index));
            }
        }
    } else {       // shrink from tail: free in LIFO order of allocation
        for(int i = NumTuples - 1; i >= 0; --i) {
            // return ptr points to memory that was copied from,
            assert(addresses[i] == alloc.free(addresses[i]));      // should not point to a different addr
            assert(! (i % AllocsPerChunk) || Gen::same(addresses[i], i));    // ptr content is not altered
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
    if (shrinkFromHead) {                  // shrink from head: free in LIFO order triggers compaction in "roughly" opposite direction
        // 1st half: chop from tail, replaced by "head" (the tail of the first chunk, to be exact)
        for(i = 0; i < NumTuples / 2; ++i, ++j) {
            auto const chunkful = i / AllocsPerChunk * AllocsPerChunk,
                 indexInsideChunk = AllocsPerChunk - 1 - (i % AllocsPerChunk);
            assert(addresses[chunkful + indexInsideChunk] == alloc.free(addresses[NumTuples - 1 - i]));
        }
        // 2nd half
        i = 0;
        for_each<const_iterator>(alloc_cref, [&i, &addresses](void const* p)
                { addresses[i++] = const_cast<void*>(p); });
        for(i = 0; i < NumTuples / 4; ++i, ++j) {
            auto const chunkful = i / AllocsPerChunk * AllocsPerChunk,
                 indexInsideChunk = AllocsPerChunk - 1 - (i % AllocsPerChunk);
            assert(addresses[chunkful + indexInsideChunk] == alloc.free(addresses[NumTuples / 2 - 1 - i]));
        }
        // free them all! See note on IterableTableTupleChunks on
        // why we need a loop of calls to iterate through.
        while(! alloc.empty()) {
            for_each<iterator>(alloc, [&alloc, &j](void* p) {      // no-ops on boundary of each compacted chunk
                    if (alloc.free(p) != nullptr) {++j;} });
        }
        assert(j == NumTuples);            // number of free() calls
    } else {       // shrink from tail: free in FIFO order triggers compaction in opposite direction
        // 1st haf: chop from head, replaced by tail
        for (i = 0; i < NumTuples / 2; ++i, ++j) {               // 1st half: compacts from tail backwards
            assert(addresses[NumTuples - 1 - i] == alloc.free(addresses[i]));
            assert(Gen::same(addresses[i], NumTuples - 1 - i));
        }
        // 2nd half: similar story. Resets addresses ptr along
        // the way
        i = 0;
        for_each<const_iterator>(alloc_cref, [&i, &addresses](void const* p)
                { addresses[i++] = const_cast<void*>(p); });
        for(i = 0; i < NumTuples / 4; ++i, ++j) {
            assert(addresses[NumTuples/2 - 1 - i] == alloc.free(addresses[i]));
        }
        // we can continue, each time free()-ing half of
        // what is left. But that is not needed since we already
        // checked twice. See documents on CompactingChunksIgnoreableFree
        // for why it need to be exception-tolerant.

        while (! alloc.empty()) {
            try {
                for_each<iterator>(alloc, [&alloc, &j](void* p) { alloc.free(p); ++j; });
            } catch (range_error const&) { }
        }
        assert(j == NumTuples);
    }
}

template<typename Alloc, typename Compactible = typename Alloc::compactibility> struct TrackedDeleter;
// compacting chunks' free() returns ptr that we will track
template<typename Alloc> struct TrackedDeleter<Alloc, integral_constant<bool, true>> {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed |= m_alloc.free(p) != nullptr;
    }
};
// non-compacting chunks' free() is void
template<typename Alloc> struct TrackedDeleter<Alloc, integral_constant<bool, false>> {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed = true;
        m_alloc.free(p);
    }
};

template<typename Chunks, size_t NthBit>
void testCustomizedIterator(size_t skipped) {      // iterator that skips on every #skipped# elems
    using Tag = NthBitChecker<NthBit>;
    using const_iterator = typename IterableTableTupleChunks<Chunks, Tag>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, Tag>::iterator;
    class Gen : public StringGen<TupleSize> {
        size_t const m_skipped;
        unsigned char* mask(unsigned char* p, size_t state)  {
            if (state % m_skipped) {
                Tag::set(p);
            } else {
                Tag::reset(p);
            }
            return p;
        }
    public:
        explicit Gen(size_t skipped): m_skipped(skipped) {}
        unsigned char* query(size_t state) override {
            return mask(StringGen<TupleSize>::query(state), state);
        }
        inline bool same(void const* dst, size_t state) {
            static unsigned char buf[TupleSize+1];
            return ! memcmp(dst, mask(StringGen<TupleSize>::query(state, buf), state), TupleSize);
        }
    } gen(skipped);

    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(gen.same(addresses[i], i));
    }
    i = 0;
    auto const& alloc_cref = alloc;
    for_each<const_iterator>(alloc_cref, [&i, &addresses, skipped](void const* p) {
                if (i % skipped == 0) {
                    ++i;
                }
                if (Chunks::compactibility::value) {
                    assert(p == addresses[i]);
                }
                ++i;
            });
    assert(i == NumTuples);
    // Free all tuples via allocator. Note that allocator needs
    // to be aware the "emptiness" from the iterator POV, not
    // allocator's POV.

    // TODO: enable them when non-compacting chunks is working on
    // it. delete crashes when iterator is advancing, pointing to invalid chunk
    if (Chunks::compactibility::value) {
        bool freed;
        TrackedDeleter<Chunks> deleter(alloc, freed);
        Tag const tag;
        do {
            freed = false;
            try {
                i = 0;
                for_each<iterator>(alloc, [&deleter, &tag, &i](void* p) {
                        assert(tag(p));
                        deleter(p);
                        ++i;                                       // on non-compacting chunk: crash when i == 4
//                        printf("%lu\n", i); fflush(stdout);
                        });
            } catch (range_error const&) {}
        } while(freed);
        // Check using normal iterator (i.e. non-skipping one) that
        // the remains are what should be.
        for_each<typename IterableTableTupleChunks<Chunks, truth>::const_iterator>(
                alloc_cref, [&tag](void const* p) { assert(! tag(p)); });
    }
}

TEST_F(TableTupleAllocatorTest, TestCompactingChunks) {
    testCompactingChunks<CompactingChunks<ShrinkDirection::head>>();
    testCompactingChunks<CompactingChunks<ShrinkDirection::tail>>();
    for (auto skipped = 8lu; skipped < 64; skipped +=8) {
        testCustomizedIterator<CompactingChunks<ShrinkDirection::head>, 3>(skipped);
        testCustomizedIterator<CompactingChunks<ShrinkDirection::tail>, 3>(skipped);
        testCustomizedIterator<NonCompactingChunks<EagerNonCompactingChunk>, 3>(skipped);
        testCustomizedIterator<NonCompactingChunks<LazyNonCompactingChunk>, 3>(skipped);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

