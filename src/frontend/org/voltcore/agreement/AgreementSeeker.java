/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.messaging.FailureSiteForwardMessage;
import org.voltcore.messaging.FailureSiteUpdateMessage;
import org.voltcore.utils.Pair;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

public class AgreementSeeker {

    protected final ArbitrationStrategy m_strategy;

    protected Set<Long> m_hsids;
    protected Set<Long> m_survivors;

    protected final TreeMultimap<Long, Long> m_reported = TreeMultimap.create();
    protected final TreeMultimap<Long, Long> m_witnessed = TreeMultimap.create();
    protected final TreeMultimap<Long, Long> m_alive = TreeMultimap.create();

    public AgreementSeeker(final ArbitrationStrategy strategy) {
        m_strategy = strategy;

        m_hsids = ImmutableSet.of();
        m_survivors = ImmutableSet.of();
    }

    public void startSeekingFor(final Set<Long> hsids, final Map<Long,Boolean> inTrouble) {
        if (!m_hsids.equals(hsids)) {
            if (!m_hsids.isEmpty()) clear();
            m_hsids = ImmutableSortedSet.copyOf(hsids);
        }
        m_survivors = m_strategy.accept(survivorPicker, Pair.of(m_hsids, inTrouble));
    }

    public void clear() {
        m_reported.clear();
        m_witnessed.clear();
        m_alive.clear();

        m_hsids = ImmutableSet.of();
        m_survivors = ImmutableSet.of();
    }

    static protected final ArbitrationStrategy.Visitor<Set<Long>, Pair<Set<Long>, Map<Long,Boolean>>> survivorPicker =
            new ArbitrationStrategy.Visitor<Set<Long>, Pair<Set<Long>,Map<Long,Boolean>>>() {

        @Override
        public Set<Long> visitMatchingCardinality(Pair<Set<Long>, Map<Long, Boolean>> p) {
            Set<Long> witnessed =
                    Maps.filterEntries(p.getSecond(), amongWitnessedHsids(p.getFirst())).keySet();
            return ImmutableSortedSet.copyOf(Sets.difference(p.getFirst(), witnessed));
        }

        @Override
        public Set<Long> visitNoQuarter(Pair<Set<Long>, Map<Long, Boolean>> p) {
            Set<Long> reported = Maps.filterKeys(p.getSecond(), in(p.getFirst())).keySet();
            return ImmutableSortedSet.copyOf(Sets.difference(p.getFirst(), reported));
        }
    };

    public static Predicate<Map.Entry<Long, Boolean>> amongWitnessedHsids(final Set<Long> hsids) {
        return new Predicate<Map.Entry<Long,Boolean>>() {
            @Override
            public boolean apply(Entry<Long, Boolean> e) {
                return hsids.contains(e.getKey()) && e.getValue();
            }
        };
    }

    public final Predicate<Long> amongSurvivors = new Predicate<Long>() {
        @Override
        public boolean apply(Long site) {
            return m_survivors.contains(site);
        }
    };

    public Set<Long> getSurvivors() {
        return m_survivors;
    }

    protected void add(FailureSiteUpdateMessage fsum, Long reportingHsid) {
        if (!m_hsids.contains(reportingHsid)) return;
        Set<Long> witnessed = Sets.newHashSet();

        for (Map.Entry<Long, Boolean> e: fsum.m_failedHSIds.entrySet()) {
            if (!m_hsids.contains(e.getKey())) continue;
            m_reported.put(e.getKey(), reportingHsid);
            if (e.getValue()) {
                m_witnessed.put(e.getKey(), reportingHsid);
                witnessed.add(e.getKey());
            }
        }
        Iterator<Map.Entry<Long, Long>> itr = m_alive.entries().iterator();
        while (itr.hasNext()) {
            Map.Entry<Long,Long> e = itr.next();
            if (e.getValue() == reportingHsid) {
                itr.remove();
            }
        }
        for (Long alive: Sets.difference(m_hsids, witnessed)) {
            m_alive.put(alive, reportingHsid);
        }
    }

    public void add(FailureSiteUpdateMessage fsum) {
        add(fsum, fsum.m_sourceHSId);
    }

    public void add(FailureSiteForwardMessage fsfm) {
        add(fsfm, fsfm.m_reportingHSId);
    }

    protected Boolean haveAgreement(Scenario sc) {
        return m_strategy.accept(agreementSeeker, sc);
    }

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

        // it matches if witnessed sets are the same ad contain all alives
        @Override
        public Boolean visitMatchingCardinality(Scenario sc) {
            boolean agree = true;
            Set<Long> quorum = sc.alive.keySet();
            for (Long w: sc.witnessed.keySet()) {
                agree = agree && quorum.equals(sc.witnessed.get(w));
            }
            return agree;
        }
    };

    public boolean needForward(long hsid) {
        return m_strategy.accept(forwardDemander, hsid);
    }

    protected ArbitrationStrategy.Visitor<Boolean, Long> forwardDemander =
            new ArbitrationStrategy.Visitor<Boolean, Long>() {

        @Override
        public Boolean visitNoQuarter(Long hsid) {
            return false;
        }

        @Override
        public Boolean visitMatchingCardinality(Long hsid) {
            Set<Long> unreachable = Sets.filter(m_hsids, not(amongSurvivors));
            Set<Long> butAlive = Sets.intersection(m_alive.keySet(), unreachable);

            return !butAlive.isEmpty()
                && !m_witnessed.get(hsid).containsAll(butAlive);
        }
    };

    public Set<Long> nextKill() {
        return m_strategy.accept(killPicker, (Void)null);
    }

    protected ArbitrationStrategy.Visitor<Set<Long>, Void> killPicker =
            new ArbitrationStrategy.Visitor<Set<Long>, Void>() {

                @Override
                public Set<Long> visitNoQuarter(Void nada) {
                    return ImmutableSet.copyOf(m_reported.keySet());
                }

                // When recomputing you need to remove all alive by (a)
                // and add them to witnessed
                // alive(a) is c,d,e,f now witnessed(a) += alive(a)
                // you also have to remove all (a)'s from witnessedBy values,
                // as (a) can no longer witness
                @Override
                public Set<Long> visitMatchingCardinality(Void nada) {
                    Scenario sc = new Scenario();
                    while (!haveAgreement(sc)) {
                        Long pick = null;
                        for (Long s: sc.witnessed.keySet()) {

                            Set<Long> witnessedBy = sc.witnessed.get(s);
                            if (witnessedBy.isEmpty()) continue;

                            if (pick != null) {
                                int cmp = witnessedBy.size()
                                        - sc.witnessed.get(pick).size();
                                if (cmp > 0 || (cmp == 0 && s > pick)) {
                                    pick = s;
                                }
                            } else {
                                pick = s;
                            }
                        }
                        if (pick == null) {
                            return ImmutableSet.of();
                        }
                        Iterator<Map.Entry<Long, Long>> itr =
                                sc.witnessed.entries().iterator();
                        while (itr.hasNext()) {
                            Map.Entry<Long,Long> e = itr.next();
                            if (e.getValue() == pick) {
                                itr.remove();
                            }
                        }
                        itr = sc.alive.entries().iterator();
                        while (itr.hasNext()) {
                            Map.Entry<Long,Long> e = itr.next();
                            if (e.getValue() == pick) {
                                itr.remove();
                            }
                        }
                        sc.witnessed.putAll(pick, sc.alive.removeAll(pick));
                    }
                    return ImmutableSet.copyOf(sc.witnessed.keySet());
                }
            };

    public Set<Long> forWhomSiteIsDead(long hsid) {
        ImmutableSet.Builder<Long> isb = ImmutableSet.builder();
        Set<Long> witnessedBy = m_witnessed.get(hsid);
        if (   !witnessedBy.isEmpty()
            && m_survivors.contains(hsid)
            && m_strategy == ArbitrationStrategy.MATCHING_CARDINALITY) {
            isb.addAll(Sets.filter(witnessedBy, amongSurvivors));
        }
        return isb.build();
    }

    protected class Scenario {

        protected TreeMultimap<Long, Long> reported;
        protected TreeMultimap<Long, Long> witnessed;
        protected TreeMultimap<Long, Long> alive;
        protected Set<Long> survivors;

        protected Scenario() {
            reported = TreeMultimap.create(m_reported);
            witnessed = TreeMultimap.create(m_witnessed);
            survivors = Sets.newTreeSet(m_survivors);
            alive = TreeMultimap.create(m_alive);
        }
    }
}
