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

#ifndef FRAGMENTMANAGER_H_
#define FRAGMENTMANAGER_H_

#include <cstring>
#include <algorithm>
#include <boost/shared_ptr.hpp>
// The next #define limits the number of features pulled into the build
// We don't use those features.
#define BOOST_MULTI_INDEX_DISABLE_SERIALIZATION
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/sequenced_index.hpp>

namespace voltdb {

const int64_t FRAGMENT_CACHE_SIZE = 1000;

/**
 * Represents a cached plan graph (as JSON string, along with fragid)
 */
struct CachedPlan {

    /**
     * Use a handle-pattern so memory allocation can be delayed until after
     * insertion. See intern().
     */
    struct Core {
        Core(const char *plan) : plan(plan), deallocOnDelete(false) {}
        ~Core() { if (deallocOnDelete) delete[] plan; }

        const char *plan;
        bool deallocOnDelete; // set true by intern()
    };

    CachedPlan(const char *plan, int32_t length, int64_t fragId)
    : core(new Core(plan)), length(length), fragmentId(fragId) {}

    // single instance of a Core shared by all copy-constructed instances
    boost::shared_ptr<Core> core;
    int32_t length; // not null terminated
    int64_t fragmentId;

    friend bool operator< (const CachedPlan &x, const CachedPlan &y);

    /** Allocate a copy from the JNI-owned memory for long-term storage */
    void intern() {
        char *copy = new char[length];
        memcpy(copy, core->plan, length);
        core->plan = copy;
        core->deallocOnDelete = true;
    }
};

/**
 * Class to keep an LRU cache of plan graphs (as JSON strings).
 * It's the VoltDBEngine's job to keep the loaded graphs in-sync
 * with this class's internal structure.
 *
 */
class FragmentManager {
private:

    /**
     * Uses a single set of nodes that both have order, as well as an index
     * on the plan bytes themselves. Here lies boost-related dragons.
     */
    typedef boost::multi_index::multi_index_container<
        CachedPlan,
        boost::multi_index::indexed_by<
            boost::multi_index::sequenced<>,
            boost::multi_index::ordered_unique<boost::multi_index::identity<CachedPlan> >
        >
    > PlanSet;

public:
    // fixed cache size
    FragmentManager() : m_nextFragmentId(-1), m_cacheSize(FRAGMENT_CACHE_SIZE) {}
    // for debugging
    FragmentManager(size_t cacheSize) : m_nextFragmentId(-1), m_cacheSize(cacheSize) {}

    /**
     * Check if a plan is in the cache.
     * If so, bump it to the most recently accessed position.
     * If not, insert at the most recently accessed position.
     * Then return true on cache hit and false on miss (needs loading).
     * It's up to the caller to load/unload plan graphs based on hit/miss.
     */
    bool upsert(const char *plan, int32_t length, int64_t &fragId) {

        CachedPlan key(plan, length, m_nextFragmentId--);

        std::pair<PlanSet::iterator,bool> p = m_plans.push_front(key);
        //if cache hit
        if (!p.second) {
            fragId = p.first->fragmentId;
            // safety check
            assert(memcmp(p.first->core->plan, plan, length) == 0);
            m_plans.relocate(m_plans.begin(),p.first);
            assert(fragId < 0);
            return true;
        }
        // if cache miss
        else {
            // only after successful insert, allocate/copy plan data
            key.intern();
            // safety check
            assert(memcmp(key.core->plan, plan, length) == 0);
            fragId = key.fragmentId;
            assert(fragId < 0);
            return false;
        }
    }

    /**
     * If the cache is over the requested size, return the frag id of
     * the graph with the oldest access time. Otherwise return 0.
     */
    int64_t purgeNext() {
        int64_t retval = 0;
        if (m_plans.size() > m_cacheSize) {
            CachedPlan plan = m_plans.back();
            retval = plan.fragmentId;
            m_plans.pop_back();
        }
        return retval;
    }

    void clear() {
        m_plans.clear();
    }

    /** Number of objects cached */
    int64_t size() {
        return static_cast<int64_t>(m_plans.size());
    }

private:
    PlanSet m_plans;
    int64_t m_nextFragmentId;
    const size_t m_cacheSize;
};

}

#endif // FRAGMENTMANAGER_H_
