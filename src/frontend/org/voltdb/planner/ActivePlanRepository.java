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

package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.Sha1Wrapper;

public abstract class ActivePlanRepository {

    /// A plan fragment entry in the cache.
    private static class FragInfo {
        final Sha1Wrapper hash;
        final long fragId;
        final byte[] plan;
        int refCount;
        /// The ticker value current when this fragment was last (dis)used.
        /// A new FragInfo or any other not in the LRU map because it is being referenced has value 0.
        /// A non-zero value is either the fragment's current key in the LRU map OR its intended/future
        /// key, if it has been lazily updated after the fragment was reused.
        long lastUse;
        /// The statement text for this fragment.  For ad hoc queries this may be null, since
        /// there is no single statement text---ad hoc queries that differ only by their constants
        /// reuse the same plan.
        String stmtText;

        FragInfo(Sha1Wrapper key, byte[] plan, long nextId, String stmtText)
        {
            this.hash = key;
            this.plan = plan;
            this.fragId = nextId;
            this.refCount = 0;
            this.lastUse = 0;
            this.stmtText = stmtText;
        }
    }

    private static HashMap<Sha1Wrapper, FragInfo> m_plansByHash = new HashMap<Sha1Wrapper, FragInfo>();
    private static HashMap<Long, FragInfo> m_plansById = new HashMap<Long, FragInfo>();
    private static TreeMap<Long, FragInfo> m_plansLRU = new TreeMap<Long, FragInfo>();
    /// A ticker that provides temporary ids for all cached fragments, for communicating with the EE.
    private static final long INITIAL_FRAG_ID = 5000;
    private static long m_nextFragId = INITIAL_FRAG_ID;
    /// A ticker that allows the sequencing of all fragment uses, providing a key to the LRU map.
    private static long m_nextFragUse = 1;

    /**
     * Get the site-local fragment id for a given plan identified by 20-byte sha-1 hash
     */
    public static long getFragmentIdForPlanHash(byte[] planHash) {
        Sha1Wrapper key = new Sha1Wrapper(planHash);
        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansByHash.get(key);
        }
        assert(frag != null);
        return frag.fragId;
    }

    /**
     * Get the statement text for the fragment identified by its hash
     */
    public static String getStmtTextForPlanHash(byte[] planHash) {
        Sha1Wrapper key = new Sha1Wrapper(planHash);
        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansByHash.get(key);
        }
        assert(frag != null);
        // SQL statement text is not stored in the repository for ad hoc statements
        // -- it may be inaccurate because we parameterize the statement on its constants.
        // Callers know if they are asking about ad hoc or pre-planned fragments,
        // and shouldn't call this method for the ad hoc case.
        assert(frag.stmtText != null);
        return frag.stmtText;
    }

    /**
     * Get the site-local fragment id for a given plan identified by 20-byte sha-1 hash
     * If the plan isn't known to this SPC, load it up. Otherwise addref it.
     */
    public static long loadOrAddRefPlanFragment(byte[] planHash, byte[] plan, String stmtText) {
        Sha1Wrapper key = new Sha1Wrapper(planHash);
        synchronized (FragInfo.class) {
            FragInfo frag = m_plansByHash.get(key);
            if (frag == null) {
                frag = new FragInfo(key, plan, m_nextFragId++, stmtText);
                m_plansByHash.put(frag.hash, frag);
                m_plansById.put(frag.fragId, frag);
                if (m_plansById.size() > ExecutionEngine.EE_PLAN_CACHE_SIZE) {
                    evictLRUfragment();
                }
            }

            // Bit of a hack to work around an issue where a statement-less adhoc
            // fragment could be identical to a statement-needing regular procedure.
            // This doesn't really address the broader issue that fragment hashes
            // are not 1-1 with SQL statements.
            if (frag.stmtText == null) {
                frag.stmtText = stmtText;
            }

            // The fragment MAY be in the LRU map.
            // An incremented refCount is a lazy way to keep it safe from eviction
            // without having to update the map.
            // This optimizes for popular fragments in a small or stable cache that may be reused
            // many times before the eviction process needs to take any notice.
            frag.refCount++;
            return frag.fragId;
        }
    }

    private static void evictLRUfragment() {
        /// Evict the least recently used fragment (if any are currently unused).
        /// Along the way, update any obsolete entries that were left
        /// by the laziness of the fragment state changes (fragment reuse).
        /// In the rare case of a cache bloated beyond its usual limit,
        /// keep evicting as needed and as entries are available until the bloat is gone.

        while ( ! m_plansLRU.isEmpty()) {
            // Remove the earliest entry.
            Entry<Long, FragInfo> lru = m_plansLRU.pollFirstEntry();
            FragInfo frag = lru.getValue();
            if (frag.refCount > 0) {
                // The fragment is being re-used, it is no longer an eviction candidate.
                // It is only in the map due to the laziness in loadOrAddRefPlanFragment.
                // It will be re-considered (at a later key) once it is no longer referenced.
                // Resetting its lastUse to 0, here, restores it to a state identical to that
                // of a new fragment.
                // It eventually causes decrefPlanFragmentById to put it back in the map
                // at its then up-to-date key.
                // This makes it safe to keep out of the LRU map for now.
                // See the comment in decrefPlanFragmentById and the one in the next code block.
                frag.lastUse = 0;
            }
            else if (lru.getKey() != frag.lastUse) {
                // The fragment is not in use but has been re-used more recently than the key reflects.
                // This is a result of the laziness in decrefPlanFragmentById.
                // Correct the entry's key in the LRU map to reflect its last use.
                // This may STILL be the least recently used entry.
                // If so, it will be picked off in a later iteration of this loop;
                // its key will now match its lastUse value.
                m_plansLRU.put(frag.lastUse, frag);
            }
            else {
                // Found and removed the actual up-to-date least recently used entry from the LRU map.
                // Remove the entry from the other collections.
                m_plansById.remove(frag.fragId);
                m_plansByHash.remove(frag.hash);
                // Normally, one eviction for each new fragment is enough to restore order.
                // BUT, if a prior call ever failed to find an unused fragment in the cache,
                // the cache may have grown beyond its normal size. In that rare case,
                // one eviction is not enough to reduce the cache to the desired size,
                // so take another bite at the apple.
                // Otherwise, trading exactly one evicted fragment for each new fragment
                // would never reduce the cache.
                if (m_plansById.size() > ExecutionEngine.EE_PLAN_CACHE_SIZE) {
                     continue;
                }
                return;
            }
        }
        // Strange. All FragInfo entries appear to be in use. There's nothing to evict.
        // Let the cache bloat a little and try again later after the next new fragment.
    }

    /**
     * Decref the plan associated with this site-local fragment id. If the refcount
     * goes to 0, the plan may be removed (depending on caching policy).
     */
    public static void decrefPlanFragmentById(long fragmentId) {
        // skip dummy/invalid fragment ids
        if (fragmentId <= 0) return;

        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansById.get(fragmentId);
            // The assert that used to be here would fail in TestAdHocQueries when it
            // re-initialized the RealVoltDB, clearing the m_plansById before
            // all SQLStmts were finalized. Maybe that's just a "test bug" that would be
            // better fixed with some kind of test-only cleanup hook?
            // OR It's possible that this early return is covering for a minor bug.
            // Maybe SQLStmt.finalize is calling this method when it shouldn't?
            // Maybe that's because the SQLStmt site member should be null in more cases?
            //assert(frag != null);
            if (frag == null) {
                return;
            }
            if (--frag.refCount == 0) {
                // The disused fragment belongs in the LRU map at the end -- at the current "ticker".
                // If its lastUse value is 0 like a new entry's, it is not currently in the map.
                // Put into the map in its proper position.
                // If it is already in the LRU map (at a "too early" entry), just set its lastUse value
                // as a cheap way to notify evictLRUfragment that it is not ready for eviction but
                // should instead be re-ordered further forward in the map.
                // This re-ordering only needs to happen when the eviction process considers the entry.
                // For a popular fragment in a small or stable cache, that may be after MANY
                // re-uses like this.
                // This prevents thrashing of the LRU map, repositioning recent entries.
                boolean notInLRUmap = (frag.lastUse == 0); // check this BEFORE updating lastUse
                frag.lastUse = ++m_nextFragUse;
                if (notInLRUmap) {
                    m_plansLRU.put(frag.lastUse, frag);
                }
            }
        }
    }

    /**
     * Get the full JSON plan associated with a given site-local fragment id.
     * Called by the EE
     */
    public static byte[] planForFragmentId(long fragmentId) {
        assert(fragmentId > 0);

        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansById.get(fragmentId);
        }
        assert(frag != null);
        return frag.plan;
    }

    @Deprecated
    public static void addFragmentForTest(long fragmentId, byte[] plan, String stmtText) {
        Sha1Wrapper key = new Sha1Wrapper(new byte[20]);
        synchronized (FragInfo.class) {
            FragInfo frag = new FragInfo(key, plan, fragmentId, stmtText);
            m_plansById.put(frag.fragId, frag);
            frag.refCount++;
        }
    }

    public static void clear() {
        synchronized (FragInfo.class) {
            m_plansById.clear();
            m_plansByHash.clear();
            m_plansLRU.clear();
            m_nextFragId = INITIAL_FRAG_ID;
            m_nextFragUse = 1;
        }
    }
}
