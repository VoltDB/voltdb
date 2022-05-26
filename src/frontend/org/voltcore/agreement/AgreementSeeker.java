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

package org.voltcore.agreement;

import static com.google_voltpatches.common.base.Predicates.in;
import static com.google_voltpatches.common.base.Predicates.not;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Multimaps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.TreeMultimap;

public class AgreementSeeker {

    protected final ArbitrationStrategy m_strategy;
    /** hsid of the site holding this instance */

    protected final long m_selfHsid;
    /** mesh hsids before agreement is sought */
    protected Set<Long> m_hsids;

    /** the set of hsids which this site can see */
    protected Set<Long> m_survivors;

    /**
     * graph where each key is a site, and their corresponding values are sites
     * that have reported it
     */
    protected final TreeMultimap<Long, Long> m_reported = TreeMultimap.create();
    /**
     * graph were each key denotes a site, and their corresponding values are sites
     * that see it dead
     */
    protected final TreeMultimap<Long, Long> m_dead = TreeMultimap.create();
    /**
     * graph were each key denotes a site, and their corresponding values are sites
     * that see it alive
     */
    protected final TreeMultimap<Long, Long> m_alive = TreeMultimap.create();

    public AgreementSeeker(final ArbitrationStrategy strategy, long selfHsid) {
        m_strategy = strategy;
        m_selfHsid = selfHsid;

        m_hsids = ImmutableSet.of();
        m_survivors = ImmutableSet.of();
    }

    /**
     * Start accumulate site links graphing information
     *
     * @param hsids pre-failure mesh hsids
     * @param inTrouble a map where each key is a failed site, and its value is
     *   a boolean that indicates whether or not the failure was witnessed directly
     *   or reported by some other site
     */
    public void startSeekingFor(
            final Set<Long> hsids, final Map<Long,Boolean> inTrouble) {

        // if the mesh hsids change we need to reset
        if (!m_hsids.equals(hsids)) {
            if (!m_hsids.isEmpty()) clear();
            m_hsids = ImmutableSortedSet.copyOf(hsids);
        }
        // determine the survivors
        m_survivors = m_strategy.accept(survivorPicker, Pair.of(m_hsids, inTrouble));
        // start accumulating link failure graphing info
        add(m_selfHsid, inTrouble);
    }

    public void clear() {
        m_reported.clear();
        m_dead.clear();
        m_alive.clear();

        m_hsids = ImmutableSet.of();
        m_survivors = ImmutableSet.of();
    }

    /**
     * Convenience method that remove all instances of the given values
     * from the given map
     * @param mm a multimap
     * @param values a set of values that need to be removed
     */
    static protected void removeValues(TreeMultimap<Long, Long> mm, Set<Long> values) {
        Iterator<Map.Entry<Long, Long>> itr = mm.entries().iterator();
        while (itr.hasNext()) {
            Map.Entry<Long, Long> e = itr.next();
            if (values.contains(e.getValue())) {
                itr.remove();
            }
        }
    }

    /**
     * a visitor that accepts a pair consisting of pre-failure mesh hsids,
     * and its failed sites map, and returns a set of survivors
     */
    static protected final ArbitrationStrategy.Visitor<Set<Long>, Pair<Set<Long>, Map<Long,Boolean>>> survivorPicker =
            new ArbitrationStrategy.Visitor<Set<Long>, Pair<Set<Long>,Map<Long,Boolean>>>() {

        @Override
        public Set<Long> visitMatchingCardinality(Pair<Set<Long>, Map<Long, Boolean>> p) {
            Set<Long> dead =
                    Maps.filterEntries(p.getSecond(), amongDeadHsids(p.getFirst())).keySet();
            return ImmutableSortedSet.copyOf(Sets.difference(p.getFirst(), dead));
        }

        @Override
        public Set<Long> visitNoQuarter(Pair<Set<Long>, Map<Long, Boolean>> p) {
            Set<Long> reported = Maps.filterKeys(p.getSecond(), in(p.getFirst())).keySet();
            return ImmutableSortedSet.copyOf(Sets.difference(p.getFirst(), reported));
        }
    };

    /**
     * returns a map entry predicate that tests whether or not the given
     * map entry describes a dead site
     * @param hsids pre-failure mesh hsids
     * @return
     */
    public static Predicate<Map.Entry<Long, Boolean>> amongDeadHsids(final Set<Long> hsids) {
        return new Predicate<Map.Entry<Long,Boolean>>() {
            @Override
            public boolean apply(Entry<Long, Boolean> e) {
                return hsids.contains(e.getKey()) && e.getValue();
            }
        };
    }

    /**
     * a site predicate that tests whether or not a site is
     * among the set of survivors
     */
    public final Predicate<Long> amongSurvivors = new Predicate<Long>() {
        @Override
        public boolean apply(Long site) {
            return m_survivors.contains(site);
        }
    };

    /**
     * returns the current set of survivors
     * @return the current set of survivors
     */
    public Set<Long> getSurvivors() {
        return m_survivors;
    }

    /**
     * Convenience method that remove all instances of the given value
     * from the given map
     * @param mm a multimap
     * @param value a value that needs to be removed
     */
    private void removeValue(TreeMultimap<Long, Long> mm, long value) {
        Iterator<Map.Entry<Long, Long>> itr = mm.entries().iterator();
        while (itr.hasNext()) {
            Map.Entry<Long,Long> e = itr.next();
            if (e.getValue().equals(value)) {
                itr.remove();
            }
        }
    }

    protected String dumpGraph(Multimap<Long,Long> mm, StringBuilder sb) {
        sb.append("{ ");
        int count = 0;
        for (long h: mm.keySet()) {
            if (count++ > 0) sb.append(", ");
            sb.append(CoreUtils.hsIdToString(h)).append(": [");
            sb.append(CoreUtils.hsIdCollectionToString(mm.get(h)));
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    public String dumpAlive() {
        StringBuilder sb = new StringBuilder();
        sb.append("Alive: ");
        dumpGraph(m_alive, sb);
        return sb.toString();
    }

    public String dumpDead() {
        StringBuilder sb = new StringBuilder();
        sb.append("Dead: ");
        dumpGraph(m_dead, sb);
        return sb.toString();
    }

    public String dumpReported() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reported: ");
        dumpGraph(m_reported, sb);
        return sb.toString();
    }

    public String dumpSurvivors() {
        StringBuilder sb = new StringBuilder();
        sb.append("Survivor: ");
        sb.append("{ ");
        int count = 0;
        for (Long hsId : m_survivors) {
            if (count++ > 0) sb.append(", ");
            sb.append(CoreUtils.hsIdToString(hsId));
        }
        sb.append(" }");
        return sb.toString();
    }

    /**
     *  Adds alive and dead graph information
     *  @param reportingHsid site reporting failures
     *  @param failures seen by the reporting site
     */
    void add(long reportingHsid, final Map<Long,Boolean> failed) {
        // skip if the reporting site did not belong to the pre
        // failure mesh
        if (!m_hsids.contains(reportingHsid)) return;

        // ship if the reporting site is reporting itself dead
        Boolean harakiri = failed.get(reportingHsid);
        if (harakiri != null && harakiri.booleanValue()) return;

        Set<Long> dead = Sets.newHashSet();

        for (Map.Entry<Long, Boolean> e: failed.entrySet()) {
            // skip if the failed site did not belong to the
            // pre failure mesh
            if (!m_hsids.contains(e.getKey())) continue;

            m_reported.put(e.getKey(), reportingHsid);
            // if the failure is witnessed add it to the dead graph
            if (e.getValue()) {
                m_dead.put(e.getKey(), reportingHsid);
                dead.add(e.getKey());
            }
        }
        // once you are witnessed dead you cannot become undead,
        // but it is not the case for alive nodes, as they can
        // die. So remove all what the reporting site thought
        // was alive before this invocation
        removeValue(m_alive, reportingHsid);

        for (Long alive: Sets.difference(m_hsids, dead)) {
            m_alive.put(alive, reportingHsid);
        }
    }

    /**
     *  Adds alive and dead graph information from a reporting
     *  site survivor set
     *  @param reportingHsid the reporting site
     *  @param sfm a {@link SiteFailureMessage} containing that
     *      site's survivor set
     */
    public void add(long reportingHsid, SiteFailureMessage sfm) {
        // skip if the reporting site did not belong to the pre
        // failure mesh, or the reporting site is reporting itself
        // dead, or none of the sites in the safe transaction map
        // are among the known hsids
        if (   !m_hsids.contains(reportingHsid)
            || !sfm.m_survivors.contains(reportingHsid)) return;

        Set<Long> survivors = sfm.m_survivors;
        if (Sets.filter(sfm.getObservedFailedSites(), in(m_hsids)).isEmpty()) {
            survivors = m_hsids;
        }

        // dead = pre failure mesh - survivors
        Set<Long> dead = Sets.difference(m_hsids, survivors);

        removeValue(m_dead, reportingHsid);

        // add dead graph nodes
        for (long w: dead) {
            if (!m_hsids.contains(w)) continue;
            m_dead.put(w,reportingHsid);
        }

        // Remove all what the reporting site thought
        // was alive before this invocation
        removeValue(m_alive, reportingHsid);

        // add alive graph nodes
        for (long s: survivors) {
            if (!m_hsids.contains(s)) continue;
            m_alive.put(s, reportingHsid);
        }

        for (long s: sfm.getFailedSites()) {
            if (!m_hsids.contains(s)) continue;
            m_reported.put(s, reportingHsid);
        }
    }

    /**
     *  Adds alive and dead graph information from a reporting
     *  site survivor set
     *  @param sfm a {@link SiteFailureMessage} containing that
     *      site's survivor set
     */
    public void add(SiteFailureMessage sfm) {
        add(sfm.m_sourceHSId, sfm);
    }

    /**
     *  Adds alive and dead graph information from a reporting
     *  site survivor set
     *  @param reportingHsid the reporting site
     *  @param sfm a {@link SiteFailureForwardMessage} containing that
     *      site's survivor set
     */
    public void add(SiteFailureForwardMessage fsfm) {
        add(fsfm.m_reportingHSId,fsfm);
    }

    /**
     * Does the given graph scenario meet the criteria of
     * having reached an agreement?
     * @param sc a graph scenario
     */
    protected Boolean haveAgreement(Scenario sc) {
        return m_strategy.accept(agreementSeeker, sc);
    }

    /**
     * a visitor that accepts a graph scenario and returns whether or not
     * the graph scenario has reached an agreement
     */
    protected final ArbitrationStrategy.Visitor<Boolean, Scenario> agreementSeeker =
            new ArbitrationStrategy.Visitor<Boolean, Scenario>() {

        @Override
        public Boolean visitNoQuarter(Scenario sc) {
            Iterator<Long> itr = sc.reported.keySet().iterator();
            boolean agree = true;
            while (agree && itr.hasNext()) {
                agree = sc.reported.get(itr.next()).containsAll(sc.survivors);
            }
            return agree;
        }

        /**
         * a quorum is comprised of sites that the given scenario
         * has left alive. This returns true if each dead node in
         * the scenario is seen dead by all in the quorum
         */
        @Override
        public Boolean visitMatchingCardinality(Scenario sc) {
            boolean agree = true;
            Set<Long> quorum = Sets.intersection(sc.alive.keySet(),sc.survivors);
            for (Long dead: sc.dead.keySet()) {
                agree = agree && quorum.equals(sc.dead.get(dead));
            }
            return agree;
        }
    };

    public boolean needForward(int [] countdown) {
        return --countdown[0] > 0 && m_strategy.accept(forwardDemander, (Void)null);
    }

    /**
     * Is anyone in the mesh alive and connected to sites I consider dead?
     */
    public boolean needForward() {
        return m_strategy.accept(forwardDemander, (Void)null);
    }

    /**
     * a visitor that tests whether or not there is a connected path
     * between myself and any site I consider dead
     */
    protected ArbitrationStrategy.Visitor<Boolean, Void> forwardDemander =
            new ArbitrationStrategy.Visitor<Boolean, Void>() {

        @Override
        public Boolean visitNoQuarter(Void nads) {
            return false;
        }

        /**
         * Tests whether or not there is a connected path between myself,
         * and any site I consider dead
         * @param nada
         * @return true if there are peer sites that can tell me about
         *    sites that I consider dead
         */
        @Override
        public Boolean visitMatchingCardinality(Void nada) {
            if (m_survivors.size() == 1) return false;

            Set<Long> unreachable = Sets.filter(m_hsids, not(in(m_survivors)));
            Set<Long> butAlive = Sets.intersection(m_alive.keySet(), unreachable);

            return !butAlive.isEmpty()
                && seenByInterconnectedPeers(butAlive, Sets.newTreeSet(m_survivors))
                && !m_dead.get(m_selfHsid).containsAll(butAlive);
        }
    };

    /**
     * Walk the alive graph to see if there is a connected path between origins,
     * and destinations
     * @param destinations set of sites that we are looking a path to
     * @param origins set of sites that we are looking a path from
     * @return true origins have path to destinations
     */
    protected boolean seenByInterconnectedPeers( Set<Long> destinations, Set<Long> origins) {
        Set<Long> seers = Multimaps.filterValues(m_alive, in(origins)).keySet();
        int before = origins.size();

        origins.addAll(seers);
        if (origins.containsAll(destinations)) {
            return true;
        } else if (origins.size() == before) {
            return false;
        }
        return seenByInterconnectedPeers(destinations, origins);
    }

    /**
     * Determine the set of nodes to kill to accomplish a fully connected
     * mesh with the remaining sites
     * @return a set of nodes to kill
     */
    public Set<Long> nextKill() {
        return m_strategy.accept(killPicker, m_selfHsid);
    }

    protected ArbitrationStrategy.Visitor<Set<Long>, Long> killPicker =
            new ArbitrationStrategy.Visitor<Set<Long>, Long>() {

                @Override
                public Set<Long> visitNoQuarter(Long self) {
                    return ImmutableSet.copyOf(m_reported.keySet());
                }

                // if a is picked then you need to remove all alive by (a)
                // and add them to dead
                // alive(a) is c,d,e,f now dead(a) += alive(a)
                // you also have to remove all (a)'s from deadBy values,
                // as (a) can no longer see anyone dead or alive
                /**
                 * This strategy picks first the sites that are considered
                 * dead by most of the remaining sites, and for ties breakers
                 * it picks the sites with highest hsids
                 * @param self the invoking site hsid
                 * @return a set of nodes to kill
                 */
                @Override
                public Set<Long> visitMatchingCardinality(Long self) {
                    Set<Long> picks = Sets.newHashSet();
                    Scenario sc = new Scenario();
                    while (!haveAgreement(sc)) {
                        Long pick = null;
                        for (Long s: sc.dead.keySet()) {

                            // cannot pick self or the ones already picked
                            if (s.equals(self) || picks.contains(s)) continue;

                            Set<Long> deadBy = sc.dead.get(s);
                            if (deadBy.isEmpty()) continue;

                            if (pick != null) {
                                int cmp = deadBy.size()
                                        - sc.dead.get(pick).size();
                                if (cmp > 0 || (cmp == 0 && s.compareTo(pick) > 0)) {
                                    pick = s;
                                }
                            } else {
                                pick = s;
                            }
                        }
                        if (pick == null) {
                            /*
                             * You only get here if and ONLY if yourself are the ONLY viable kill
                             */
                            return ImmutableSet.of();/*ImmutableSet.copyOf(
                                    Sets.filter(m_hsids, not(equalTo(m_selfHsid))));*/
                        }

                        // pick can no longer see anyone dead or alive
                        removeValue(sc.dead,pick);
                        removeValue(sc.alive, pick);

                        sc.dead.putAll(pick, sc.alive.removeAll(pick));

                        picks.add(pick);
                    }
                    return ImmutableSet.copyOf(sc.dead.keySet());
                }
            };


    /**
     * Is the given hsid considered dead by anyone in my survivor set?
     * @param hsid a site hsid
     * @return a subset of my survivor set that considers the given site dead
     */
    public Set<Long> forWhomSiteIsDead(long hsid) {
        ImmutableSet.Builder<Long> isb = ImmutableSet.builder();
        Set<Long> deadBy = m_dead.get(hsid);
        if (   !deadBy.isEmpty()
            && m_survivors.contains(hsid)
            && m_strategy == ArbitrationStrategy.MATCHING_CARDINALITY) {
            isb.addAll(Sets.filter(deadBy, amongSurvivors));
        }
        return isb.build();
    }

    public long bestKillCandidateAmong(Set<Long> candidates) {
        long pick = Long.MIN_VALUE;
        int dings = Integer.MIN_VALUE;
        for (long candidate: candidates) {
            int siteDings = m_dead.get(candidate).size();
            if (siteDings > dings) {
                dings = siteDings;
                pick = candidate;
            } else if (dings == siteDings && candidate > pick) {
                pick = candidate;
            }
        }
        return pick;
    }

    public boolean alreadyKnow(FaultMessage fm) {
        for (Long survivor : fm.survivors) {
            // Do we already know all the survivors?
            if (!m_alive.get(survivor).contains(fm.reportingSite)) {
                return false;
            }
        }
        // Do we already know the dead?
        if (!m_dead.get(fm.failedSite).contains(fm.reportingSite)) {
            return false;
        }
        // Nothing new!! No need to report to fault resolver.
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AgreementSeeker { hsId: ").append(CoreUtils.hsIdToString(m_selfHsid));
        sb.append(", survivors: [").append(CoreUtils.hsIdCollectionToString(m_survivors));
        sb.append("], alive: ");
        dumpGraph(m_alive, sb);
        sb.append(", dead: ");
        dumpGraph(m_dead, sb);
        sb.append("}");
        return sb.toString();
    }

    protected class Scenario {

        protected TreeMultimap<Long, Long> reported;
        protected TreeMultimap<Long, Long> dead;
        protected TreeMultimap<Long, Long> alive;
        protected Set<Long> survivors;

        protected Scenario() {
            reported = TreeMultimap.create(m_reported);
            dead = TreeMultimap.create(m_dead);
            survivors = Sets.newTreeSet(m_survivors);
            alive = TreeMultimap.create(m_alive);
        }
    }
}
