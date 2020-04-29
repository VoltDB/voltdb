/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
#include <cstdarg>
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

template<size_t N> using varray = array<void const*, N>;
template<typename Alloc> void* remove_single(Alloc& alloc, void const* p) {
    alloc.remove_reserve(1);
    alloc.remove_add(const_cast<void*>(p));
    void* r = nullptr;
    assert(1 ==
            alloc.template remove_force<truth>([&r](vector<pair<void*, void const*>> const& entries) noexcept {
                if (! entries.empty()) {
                    assert(entries.size() == 1);
                    r = memcpy(entries[0].first, entries[0].second, TupleSize);
                }
            }).first);
    return r;
}

template<typename Alloc> pair<size_t, size_t> remove_multiple(Alloc& alloc, size_t n, ...) {
    alloc.remove_reserve(n);
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; ++i) {
        alloc.remove_add(const_cast<void*>(va_arg(args, void const*)));
    }
    return alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept {
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                });
}

template<typename Alloc, size_t N> pair<size_t, size_t> remove_multiple(
        Alloc& alloc, varray<N> const& addr) {
    alloc.remove_reserve(N);
    for_each(addr.cbegin(), addr.cend(),
            [&alloc](void const* p) { alloc.remove_add(const_cast<void*>(p)); });
    return alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept {
            for_each(entries.begin(), entries.end(),
                    [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
}

template<typename Alloc, typename Iter> pair<size_t, size_t> remove_multiple(
        Alloc& alloc, Iter const& beg, Iter const& end) {
    alloc.remove_reserve(distance(beg, end));
    for_each(beg, end, [&alloc](void const* p) { alloc.remove_add(const_cast<void*>(p)); });
    return alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept {
            for_each(entries.begin(), entries.end(),
                    [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            });
}

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
    varray<NumTuples> addresses;
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
                alloc.free(const_cast<void*>(addresses[j]));
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
    varray<3 * AllocsPerChunk> addresses;
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
    using addresses_type = varray<NumTuples>;
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
    varray<NumTuples> addresses;
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
}

TEST_F(TableTupleAllocatorTest, TestCompactingChunks) {
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

/**
 * Test of HookedCompactingChunks using its RW iterator that
 * effects (GC) as snapshot process continues.
 */
template<typename Chunk, gc_policy pol>
void testHookedCompactingChunks() {
    using Hook = TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    varray<NumTuples> addresses;
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
        remove_single(alloc, addresses[i]);
    }
    verify_snapshot_const();

    // Step 3: batch deletion
    remove_multiple(alloc, next(addresses.cbegin(), 909), next(addresses.cbegin(), 999));
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
        remove_single(alloc, addresses[i]);
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
                        remove_single(alloc, addresses[ii]);
                        latest[ii] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {             // if we tried to delete a non-existent address, pick another option
                        alloc.remove_reset();
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
                for_each(tb_removed.cbegin(), tb_removed.cend(),
                        [&alloc](void* p) { alloc.remove_add(p); });
                assert(alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries){
                            for_each(entries.begin(), entries.end(),
                                    [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                            }).first == tb_removed.size());
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
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    varray<AllocsPerChunk> addresses;
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
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
    };
    // batch remove last 10 entries
    remove_multiple(alloc, addresses.crbegin(), next(addresses.crbegin(), 10));
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single2() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    varray<AllocsPerChunk> addresses;
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
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
    };
    remove_multiple(alloc, addresses.cbegin(), next(addresses.cbegin(), 10));       // batch remove first 10 entries -- triggers compaction
    for (i = 0; i < 10; ++i) {       // inserts another 10 different entries
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single2a() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    varray<AllocsPerChunk> addresses;
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
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk);
    };
    remove_multiple(alloc, addresses.cbegin(), next(addresses.cbegin(), 10));       // batch remove LAST 10 entries -- no compaction
    for (i = 0; i < 10; ++i) {       // inserts another 10 different entries
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single3() {         // correctness on txn view: single elem remove
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    varray<10> addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < 10; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    remove_single(alloc, addresses[4]);                        // 9 => 4
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
    remove_single(alloc, alloc.allocate());
    assert(alloc.empty());
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_multi1() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    varray<AllocsPerChunk * 3> addresses;
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
                assert(Gen::same(p, i++));
            });
        assert(i == AllocsPerChunk * 3);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
                assert(Gen::same(p, i++));
                });
        assert(i == AllocsPerChunk * 3);
    };
    alloc.template freeze<truth>();

    auto iter = addresses.begin();
    alloc.remove_reserve(60);
    for (i = 0; i < 3; ++i) {
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
                alloc.remove_add(const_cast<void*>(p));
            });
        advance(iter, AllocsPerChunk - 10);
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
                alloc.remove_add(const_cast<void*>(p));
            });
        advance(iter, 10);
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
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
    Gen gen;
    Alloc alloc(TupleSize);
    varray<NumTuples> addresses;
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
                assert(Gen::same(p, i++));
            });
        assert(i == NumTuples);
    };
    alloc.template freeze<truth>();
    i = 0;
    remove_multiple(alloc, accumulate(addresses.cbegin(), addresses.cend(),     // remove every other
                varray<NumTuples / 2>{{}}, [&i](varray<NumTuples / 2>& acc, void const* p) {
                    if (i % 2 == 0) {
                        acc[i / 2] = p;
                    }
                    ++i;
                    return acc;
                }));
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

TEST_F(TableTupleAllocatorTest, testHookedCompactingChunksBatchRemove_nonfull_2chunks) {
    using HookAlloc = NonCompactingChunks<LazyNonCompactingChunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<gc_policy::never>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Alloc alloc(TupleSize);
    varray<AllocsPerChunk * 2 - 2> addresses; // 2 chunks, 2nd 2 allocs from full
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < addresses.size(); ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    remove_multiple(alloc, addresses.cbegin(), next(addresses.cbegin(), AllocsPerChunk + 2));
    ASSERT_EQ(AllocsPerChunk - 4, alloc.size());
}

TEST_F(TableTupleAllocatorTest, testHookedCompactingChunksStatistics) {
    using HookAlloc = NonCompactingChunks<LazyNonCompactingChunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<gc_policy::never>>;
    using Alloc = HookedCompactingChunks<Hook>;
    auto constexpr N = AllocsPerChunk * 3 + 2;
    varray<N> addresses;
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
    ASSERT_EQ(make_pair(2lu, 0lu),             // single remove, twice
            remove_multiple(alloc, 2, addresses[0], addresses[1]));
    ASSERT_EQ(4, alloc.chunks());
    ASSERT_EQ(N - 2, alloc.size());
    // batch remove last 30 entries, compacts/removes head chunk
    remove_multiple(alloc, addresses.crbegin(), next(addresses.crbegin(), 30));
    ASSERT_EQ(3, alloc.chunks());
    ASSERT_EQ(N - AllocsPerChunk, alloc.size());
}

template<typename Chunk, gc_policy pol> struct TestHookedCompactingChunks2 {
    inline void operator()() const {
        testHookedCompactingChunks<Chunk, pol>();
        // batch removal tests assume head-compacting direction
        testHookedCompactingChunksBatchRemove_single1<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single2<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single2a<Chunk, pol>();
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
    Gen gen;
    Alloc alloc(TupleSize);
    varray<NumTuples> addresses;
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
        assert(Gen::same(*iter.second, iter.first));
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
                        remove_single(alloc, addresses[i1]);
                        addresses[i1] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {                 // if we tried to delete a non-existent address, pick another option
                        alloc.remove_reset();
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
    Gen gen;
    Alloc alloc(TupleSize);
    varray<Number> addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < Number; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();                            // single chunk, not full before freeze,
    assert(make_pair(4lu, 0lu) ==                              // then a few deletions
            remove_multiple(alloc, 4, addresses[0], addresses[5], addresses[10], addresses[20]));
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
    Alloc alloc(TupleSize);
    varray<NumTuples> addresses;
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
    ASSERT_EQ(nullptr, remove_single(alloc, addr));
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

TEST_F(TableTupleAllocatorTest, TestBatchRemoveBug) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    auto const ordered_pred = [this] (Alloc const& a, size_t i, set<size_t> const& holes) {
        fold<typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator>(
                a, [this, &i, &holes] (void const* p) {
                    if (! holes.count(i)) {
                        ASSERT_EQ(i++, Gen::of(reinterpret_cast<unsigned char const*>(p)));
                    }
                });
    };
    Alloc alloc(TupleSize);
    Gen gen;
    varray<AllocsPerChunk * 5> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 5; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    // prep work: batch remove 1st .. (AllocsPerChunk - 1) -th
    // alloc, making 1st chunk contain only a single tuple
    ordered_pred(alloc, 0, {});
    ASSERT_EQ(make_pair(AllocsPerChunk - 1, 0lu),
            remove_multiple(alloc, addresses.cbegin(), next(addresses.cbegin(), AllocsPerChunk - 1)));
    // verify in-order
    ordered_pred(alloc, AllocsPerChunk - 1, {});
    ASSERT_EQ(AllocsPerChunk - 1, Gen::of(reinterpret_cast<unsigned char const*>(addresses[0])));
    // real test: remove the next (AllocsPerChunk + 2) allocs,
    // affecting first 3 chunks
    alloc.remove_reserve(AllocsPerChunk + 2);
    for (i = AllocsPerChunk; i < AllocsPerChunk * 2; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    alloc.remove_add(const_cast<void*>(addresses[0]));
    alloc.remove_add(const_cast<void*>(addresses[AllocsPerChunk * 3 - 1]));
    ASSERT_EQ(make_pair(AllocsPerChunk + 2, 0lu),
                alloc.template remove_force<truth>([this](vector<pair<void*, void const*>> const& entries) noexcept {
                        ASSERT_TRUE(entries.empty());
                    }));
    ordered_pred(alloc, AllocsPerChunk * 2, {AllocsPerChunk * 3 - 1});
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
    varray<NumTuples> addresses;
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
            remove_single(alloc, addresses[i++]);
        } catch (range_error const&) {                         // OK bc. compaction
            alloc.remove_reset();
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
    varray<NumTuples> addresses;
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
        remove_single(alloc, addresses[NumTuples - i - 1]);
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
    varray<NumTuples> addresses;
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
    varray<NumTuples> addresses;
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
    varray<NumTuples> addresses;
    size_t i;
    for (i = 0; i < NumTuples; ++i) {
        addresses[i] = memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    // Remove last 10, making 1st chunk non-full
    remove_multiple(alloc, addresses.crbegin(), next(addresses.crbegin(), 10));
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
    varray<NumTuples> addresses;
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
        remove_single(alloc, addresses[i]);
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
    using map_type = unordered_map<size_t, size_t>;
    map_type m_finalized{}, m_copied{};
    static size_t add(map_type& m, void const* p) {
        auto const key = Gen::of(reinterpret_cast<unsigned char const*>(p));
        auto iter = m.find(key);
        if (iter == m.cend()) {
            return m.emplace_hint(iter, key, 1lu)->second;
        } else {
            return ++iter->second;
        }
    }
    static size_t value_of(map_type const& m, void const* p) {
        return value_of(m, Gen::of(reinterpret_cast<unsigned char const*>(p)));
    }
    static size_t value_of(map_type const& m, size_t key) {
        auto const& iter = m.find(key);
        return iter == m.cend() ? 0 : iter->second;
    }
    // value histogram
    static map_type distribution(map_type const& m) {
        return accumulate(
                m.cbegin(), m.cend(), map_type{},
                [](map_type& acc, typename map_type::value_type const& entry) {
                    auto const& v = entry.second;
                    auto iter = acc.find(v);
                    if (iter == acc.cend()) {
                        acc.emplace_hint(iter, v, 1lu);
                    } else {
                        ++iter->second;
                    }
                    return acc;
                });
    }
public:
    finalize_verifier(size_t n) : m_total(n) {
        assert(m_total > 0);
        m_finalized.reserve(m_total);
        m_copied.reserve(m_total);
    }
    void reset(size_t n) {
        assert(n > 0);
        const_cast<size_t&>(m_total) = n;
        m_finalized.clear();
        m_finalized.reserve(n);
    }
    void operator()(void const* p) {
        bool const tst = add(m_finalized, p) <= value_of(m_copied, p) + 1;
        assert(tst);
    }
    void* operator()(void* dst, void const* src) {
        // copier
        add(m_copied, src);                                                            // TODO: finalize???
        return memcpy(dst, src, TupleSize);
    }
    map_type const& finalized() const noexcept {
        return m_finalized;
    }
    bool ok(size_t start) const {
        // find any entry that had been copied more times than finalized
        auto mismatched = accumulate(
                    m_finalized.cbegin(), m_finalized.cend(),
                    make_pair(set<size_t>{}, set<size_t>{}),        // 1. not-finalized 2. over-finalized
                    [this](pair<set<size_t>, set<size_t>>& acc,
                            typename map_type::value_type const& entry) {
                        auto &under_finalized = acc.first, &over_finalized = acc.second;
                        auto const copied = value_of(m_copied, entry.first);
                        if (copied + 1 > entry.second) {
                            under_finalized.emplace(entry.first);
                        } else if (copied + 1 < entry.second) {
                            over_finalized.emplace(entry.first);
                        }
                        return acc;
                    });
        // not-finalized set also need to check entries in m_copied but not in m_finalized
        auto& under_finalized = mismatched.first;
        for_each(m_copied.cbegin(), m_copied.cend(),
                [&under_finalized, this](typename map_type::value_type const& entry) {
                    if (! m_finalized.count(entry.first)) {
                        under_finalized.insert(entry.first);
                    }
                });
        auto const& over_finalized = mismatched.second;
        if (m_finalized.size() != m_total || ! over_finalized.empty() || ! under_finalized.empty()) {
//            copy(under_finalized.begin(), under_finalized.end(), ostream_iterator<size_t>(cout, ", "));
//            printf("=> %lu\n", under_finalized.size());
            return false;
        } else {
            assert(! m_finalized.empty());
            auto const fst = m_finalized.cbegin()->first;
            auto bounds = make_pair(fst, fst);
            for_each(m_finalized.cbegin(), m_finalized.cend(), [&bounds](typename map_type::value_type const& entry) {
                        bounds.first = min(bounds.first, entry.first);
                        bounds.second = max(bounds.second, entry.first);
                    });
            return bounds.first == start && bounds.first + m_total - 1 == bounds.second;
        }
    }
    // debug helper
    map_type finalized_distribution() const {
        return distribution(m_finalized);
    }
    map_type copied_distribution() const {
        return distribution(m_copied);
    }
    static string string_of(map_type const& m) {
        auto const r = accumulate(m.cbegin(), m.cend(), string{},
                [](string& acc, typename map_type::value_type const& entry) {
                    return acc.append(to_string(entry.first))
                            .append(" => ")
                            .append(to_string(entry.second))
                            .append(", ");
                });
        if (! r.empty()) {
            return r.substr(0, r.length() - 2);
        } else {
            return r;
        }
    }
};

TEST_F(TableTupleAllocatorTest, TestFinalizer_AllocsOnly) {
    // test allocation-only case
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    Alloc alloc(TupleSize, {
            [&verifier](void const* p) { verifier(p); },
            [&verifier](void* dst, void const* src) { return verifier(dst, src); }
        });
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
    {
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
        varray<NumTuples> addresses;
        size_t i;
        for (i = 0; i < NumTuples; ++i) {
            addresses[i] = gen.fill(alloc.allocate());
        }
        // batch remove every other tuple
        alloc.remove_reserve(NumTuples / 2);
        for (i = 0; i < NumTuples; i += 2) {
            alloc.remove_add(const_cast<void*>(addresses[i]));
        }
        ASSERT_EQ(make_pair(NumTuples / 2, NumTuples / 4),
                alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept {
                        for_each(entries.begin(), entries.end(),
                                [](pair<void*, void const*> const& entry) {
                                    memcpy(entry.first, entry.second, TupleSize);
                                });
                    }));
        // only the non-compacting subset of removal batch are finalized
        ASSERT_EQ(NumTuples / 4, verifier.finalized().size());
        for (i = 0; i < NumTuples / 2; i += 2) {
            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
        }
        // Cheat the compacting subset of removal batch
        unsigned char buf[TupleSize];
        for (i = NumTuples / 2; i < NumTuples; i += 2) {
            verifier(Gen::of(i, buf));
        }
    }
    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_FrozenRemovals) {
    // test batch removal when frozen, then thaw.
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    varray<NumTuples> addresses;
    size_t i;
    {
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
        for (i = 0; i < NumTuples; ++i) {
            addresses[i] = gen.fill(alloc.allocate());
        }
        alloc.template freeze<truth>();
        alloc.remove_reserve(NumTuples / 2);
        for (i = 0; i < NumTuples; i += 2) {
            alloc.remove_add(const_cast<void*>(addresses[i]));
        }
        ASSERT_EQ(make_pair(NumTuples / 2, 0lu),
                alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept {
                        for_each(entries.begin(), entries.end(),
                                [](pair<void*, void const*> const& entry) {
                                    memcpy(entry.first, entry.second, TupleSize);
                                });
                    }));
        // finalizer is never called in frozen state
        ASSERT_TRUE(verifier.finalized().empty());
        alloc.template thaw<truth>();
        // manually finalize on the compacted subset of the batch
        unsigned char buf[TupleSize];
        for (i = NumTuples / 2; i < NumTuples; i += 2) {
            verifier(Gen::of(i, buf));
        }
        ASSERT_EQ(NumTuples * 3 / 4, verifier.finalized().size());
        for (i = 0; i < NumTuples; i += 2) {
            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
        }
    }
    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_AllocAndUpdates) {
    // test updates when frozen, then thaw
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples + NumTuples / 2};
    {
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
        varray<NumTuples> addresses;
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
        ASSERT_TRUE(verifier.finalized().empty());                         // updates in frozen never finalize anything
        alloc.template thaw<truth>();
        ASSERT_EQ(NumTuples / 2, verifier.finalized().size());
        for (i = 0; i < NumTuples; i += 2) {
            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
        }
        // manually finalize updated addresses
        unsigned char buf[TupleSize];
        for (i = 0; i < NumTuples; i += 2) {
            verifier(Gen::of(i, buf));
        }
    }
    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_InterleavedIterator) {
    // test allocation-only case
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples + NumTuples / 2};
    Alloc alloc(TupleSize, {
            [&verifier](void const* p) { verifier(p); },
            [&verifier](void* dst, void const* src) { return verifier(dst, src); }
        });
    varray<NumTuples> addresses;
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
    ASSERT_TRUE(verifier.finalized().empty());
    // delete the second half; but only half of those deleted
    // batch are "fresh", so only 1/8 of the whole gets to be
    // finalized
    alloc.remove_reserve(NumTuples / 2);
    // but interleaved with advancing snapshot iterator to 3/4 of
    // the whole course
    for (i = 0; i < NumTuples / 4; ++i) {
        ++*iter;
    }
    for (i = 0; i < NumTuples / 2; ++i) {                                  // remove 2nd half
        alloc.remove_add(const_cast<void*>(addresses[NumTuples - i - 1]));
    }
    // finalize called on the 2nd half in the txn memory
    ASSERT_EQ(make_pair(NumTuples / 2, 0lu),
            alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries){
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
            }));
    ASSERT_TRUE(verifier.finalized().empty());
    alloc.template thaw<truth>();
//    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
//            static_cast<Alloc const&>(alloc), [&verifier](void const* p) {
//                assert(verifier.finalized().cend() == verifier.finalized().find(
//                            Gen::of(reinterpret_cast<unsigned char const*>(p))));
//            });
    alloc.template clear<truth>();
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_SimpleDtor) {
    // test that dtor should properly finalize
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    {
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
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
        varray<NumTuples + AllocsPerChunk * 2> addresses;
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
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
        varray<AllocsPerChunk * 4> buf;
        copy(addresses.cbegin(), next(addresses.cbegin(), AllocsPerChunk * 3), buf.begin());
        copy(next(addresses.cbegin(), NumTuples), next(addresses.cbegin(), NumTuples + AllocsPerChunk),
                next(buf.begin(), AllocsPerChunk * 3));
        ASSERT_EQ(make_pair(AllocsPerChunk * 4, 0lu), remove_multiple(alloc, buf));

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
        ASSERT_TRUE(verifier.finalized().empty());
        alloc.template thaw<truth>();
        ASSERT_EQ(AllocsPerChunk * 5, verifier.finalized().size());
        // batch removal on first 3 chunks: no compaction
        for (i = 0; i < AllocsPerChunk * 3; ++i) {
            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
        }
        // update of 10th chunk
        for (i = AllocsPerChunk * 10; i < AllocsPerChunk * 11; ++i) {
            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
        }
        // batch removal on 2nd to last chunk: this chunk is
        // finalized as soon as remove_add is called (as verified
        // above)
//        for (i = NumTuples; i < NumTuples + AllocsPerChunk; ++i) {
//            ASSERT_NE(verifier.finalized().cend(), verifier.finalized().find(i));
//        }
    }
//    ASSERT_TRUE(verifier.ok(0));
}

TEST_F(TableTupleAllocatorTest, TestFinalizer_bug1) {
    // test finalizer on iterator
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    using Gen = StringGen<TupleSize>;
    Gen gen;
    finalize_verifier verifier{NumTuples};
    {
        varray<NumTuples> addresses;
        Alloc alloc(TupleSize, {
                [&verifier](void const* p) { verifier(p); },
                [&verifier](void* dst, void const* src) { return verifier(dst, src); }
            });
        size_t i;
        for (i = 0; i < NumTuples; ++i) {
            addresses[i] = gen.fill(alloc.allocate());
        }
        alloc.template freeze<truth>();
        auto const batch_size = NumTuples / 2 + 4;
        ASSERT_EQ(make_pair(batch_size, 0lu),
                remove_multiple(alloc, addresses.crbegin(), next(addresses.crbegin(), batch_size)));
        alloc.template thaw<truth>();
        ASSERT_EQ(NumTuples, verifier.finalized().size());
        // TODO: with care, manually finalize copy-overed tuples
//        unsigned char buf[TupleSize];
//        for (i = NumTuples / 2 + 4; i < NumTuples; ++i) {
//            verifier(Gen::of(i, buf));
//        }
    }
//    ASSERT_TRUE(verifier.ok(0));
}

//TEST_F(TableTupleAllocatorTest, TestSimulateDuplicateSnapshotRead_mt) {
//    // test finalizer on iterator
//    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
//    using Gen = StringGen<TupleSize>;
//    Gen gen;
//    Alloc alloc(TupleSize);
//    constexpr size_t BigNumTuples = NumTuples * 5;
//    varray<Tuples> addresses;
//    for (size_t i = 0; i < BigNumTuples; ++i) {
//        addresses[i] = gen.fill(alloc.allocate());
//    }
//    auto const& iter = alloc.template freeze<truth>();
//    // deleting thread that triggers massive chained compaction,
//    // by deleting one tuple at a time, in the compacting direction.
//    // Synchronized perfectly with the snapshot iterator thread
//    // on each deletion
//    auto const deleting_thread = [&alloc, &addresses, this] () {
//        int j = 0;
//        do {
//            for (int i = j + AllocsPerChunk - (j == 0 ? 2 : 1); i >= j; --i) {
//                alloc.remove_reserve(1);
//                alloc.remove_add(const_cast<void*>(addresses[i]));
//                ASSERT_EQ(1, alloc.template remove_force<truth>([this] (vector<pair<void*, void const*>> const& entries) {
//                                ASSERT_EQ(1, entries.size());
//                                ASSERT_EQ(AllocsPerChunk - 1,
//                                        Gen::of(reinterpret_cast<unsigned char*>(entries[0].second)));
//                                memcpy(entries[0].first, entries[0].second, TupleSize);
//                            }).first);
//            }
//        } while ((j += AllocsPerChunk) < BigNumTuples);
//        ASSERT_EQ(1, alloc.size());
//    };
//    // snapshot thread that validates. Synchronized perfectly
//    // with deleting thread on each advancement
//    auto const snapshot_thread = [&iter, this] () {
//        auto iterating_counter = 0lu;
//        while (! iter->drained()) {
//            ASSERT_EQ(iterating_counter, Gen::of(reinterpret_cast<unsigned char*>(**iter)));
//            ++(*iter);
//            ++iterating_counter;
//            if (iter->drained()) {
//                break;
//            }
//        }
//        ASSERT_EQ(BigNumTuples, iterating_counter);
//    };
//    thread t1(deleting_thread);
//    snapshot_thread();
//    t1.join();
//    alloc.template thaw<truth>();
//}

TEST_F(TableTupleAllocatorTest, TestSnapIterBug_rep1) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    Alloc alloc(TupleSize);
    using Gen = StringGen<TupleSize>;
    Gen gen;
    varray<AllocsPerChunk * 3> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 3 ; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // delete last 2 tuples from last chunk
    ASSERT_EQ(make_pair(2lu, 0lu), remove_multiple(alloc, addresses.crbegin(), next(addresses.crbegin(), 2)));
    // then, freeze, and remove 1 full chunk worth of tuples
    auto const& iter = alloc.template freeze<truth>();
    alloc.remove_reserve(AllocsPerChunk);
    for (i = 0; i < AllocsPerChunk - 2; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 2; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i + AllocsPerChunk]));
    }
    ASSERT_EQ(make_pair(AllocsPerChunk, 0lu),
            alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
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
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    Alloc alloc(TupleSize);
    using Gen = StringGen<TupleSize>;
    Gen gen;
    varray<AllocsPerChunk * 3> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 3 ; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // delete last 2 tuples from 1st chunk
    ASSERT_EQ(make_pair(2lu, 0lu),
            remove_multiple(alloc, 2, addresses[AllocsPerChunk - 1], addresses[AllocsPerChunk - 2]));
    // then, freeze, and remove 1 full chunk worth of tuples
    auto const& iter = alloc.template freeze<truth>();
    alloc.remove_reserve(AllocsPerChunk);
    for (i = 0; i < AllocsPerChunk - 2; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 2; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i + AllocsPerChunk]));
    }
    ASSERT_EQ(make_pair(AllocsPerChunk, 0lu),
            alloc.template remove_force<truth>([](vector<pair<void*, void const*>> const& entries) noexcept{
                for_each(entries.begin(), entries.end(),
                        [](pair<void*, void const*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
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

TEST_F(TableTupleAllocatorTest, TestSnapIterEmptyTxnView) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
    Alloc alloc(TupleSize);
    using Gen = StringGen<TupleSize>;
    Gen gen;
    varray<AllocsPerChunk * 3> addresses;
    size_t i;
    for (i = 0; i < AllocsPerChunk * 3 ; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    // after freeze, empty txn view and use snapshot rw iterator
    auto const& iter = alloc.template freeze<truth>();
    alloc.template clear<truth>();
    i = 0;
    while (! iter->drained()) {
        ASSERT_EQ(i++, Gen::of(reinterpret_cast<unsigned char*>(**iter)));
        ++(*iter);
    }
    ASSERT_EQ(AllocsPerChunk * 3, i);
    alloc.template thaw<truth>();
}

#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}

