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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

    protected final AgreementStrategy m_strategy;
    protected final Set<Long> m_hsids;
    protected final TreeMultimap<Long, Long> m_reported = TreeMultimap.create();
    protected final TreeMultimap<Long, Long> m_witnessed = TreeMultimap.create();

    protected final Set<Long> m_survivors;

    public AgreementSeeker(
            final AgreementStrategy strategy,
            final Set<Long> hsids,
            final Map<Long,Boolean> inTrouble) {

        m_strategy = strategy;
        m_hsids = ImmutableSortedSet.copyOf(hsids);
        m_survivors = strategy.accept(survivorPicker, Pair.of(m_hsids, inTrouble));
    }

    static protected final AgreementStrategy.Visitor<Set<Long>, Pair<Set<Long>, Map<Long,Boolean>>> survivorPicker =
            new AgreementStrategy.Visitor<Set<Long>, Pair<Set<Long>,Map<Long,Boolean>>>() {

        @Override
        public Set<Long> visitMatchingCardinality(Pair<Set<Long>, Map<Long, Boolean>> p) {
            HashSet<Long> survivors = new HashSet<Long>(p.getFirst());
            survivors.removeAll(Maps.filterValues(p.getSecond(), onlyWitnessed).keySet());
            return ImmutableSortedSet.copyOf(survivors);
        }

        @Override
        public Set<Long> visitNoQuarter(Pair<Set<Long>, Map<Long, Boolean>> p) {
            HashSet<Long> survivors = new HashSet<Long>(p.getFirst());
            survivors.removeAll(p.getSecond().keySet());
            return ImmutableSortedSet.copyOf(survivors);
        }
    };

    public static final Predicate<Boolean> onlyWitnessed = new Predicate<Boolean>() {
        @Override
        public boolean apply(Boolean w) {
            return w;
        }
    };

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

        for (Map.Entry<Long, Boolean> e: fsum.m_failedHSIds.entrySet()) {
            if (!m_hsids.contains(e.getKey())) continue;
            m_reported.put(e.getKey(), reportingHsid);
            if (e.getValue()) {
                m_witnessed.put(e.getKey(), reportingHsid);
            }
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

    public Boolean haveAgreement() {
        return haveAgreement(m_scenario);
    }

    protected final AgreementStrategy.Visitor<Boolean, Scenario> agreementSeeker =
            new AgreementStrategy.Visitor<Boolean, Scenario>() {

        @Override
        public Boolean visitNoQuarter(Scenario sc) {
            Iterator<Long> itr = sc.reported.keySet().iterator();
            boolean agree = true;
            while (agree && itr.hasNext()) {
                agree = sc.reported.get(itr.next()).containsAll(sc.survivors);
            }
            return agree;
        }

        @Override
        public Boolean visitMatchingCardinality(Scenario sc) {
            Iterator<Long> itr = sc.witnessed.keySet().iterator();
            boolean agree = true;
            while (agree && itr.hasNext()) {
                agree = sc.witnessed.get(itr.next()).containsAll(sc.survivors);
            }
            return agree;
        }
    };

    public boolean needForward(long hsid) {
        return m_strategy.accept(forwardDemander, hsid);
    }

    protected AgreementStrategy.Visitor<Boolean, Long> forwardDemander =
            new AgreementStrategy.Visitor<Boolean, Long>() {

        @Override
        public Boolean visitNoQuarter(Long hsid) {
            return false;
        }

        @Override
        public Boolean visitMatchingCardinality(Long hsid) {
            if (!m_reported.containsKey(hsid)) {
                return false;
            } else {
                return !m_witnessed.containsKey(hsid);
            }
        }
    };

    protected final Scenario m_scenario = new Scenario();

    public Set<Long> nextKill() {
        return m_strategy.accept(killPicker, (Void)null);
    }

    protected AgreementStrategy.Visitor<Set<Long>, Void> killPicker =
            new AgreementStrategy.Visitor<Set<Long>, Void>() {

                @Override
                public Set<Long> visitNoQuarter(Void nada) {
                    return ImmutableSet.copyOf(m_reported.keySet());
                }

                @Override
                public Set<Long> visitMatchingCardinality(Void nada) {
                    Scenario sc = m_scenario.clone();
                    while (!haveAgreement(sc)) {
                        Long pick = null;
                        for (Long s: sc.survivors) {

                            Set<Long> witnessedBy = sc.witnessed.get(s);
                            if (witnessedBy == null) continue;

                            if (pick != null) {
                                int cmp = witnessedBy.size()
                                        - sc.witnessed.get(s).size();
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
                        sc.survivors.remove(pick);
                        sc.witnessed.putAll(pick, sc.survivors);
                    }
                    return ImmutableSet.copyOf(sc.witnessed.keySet());
                }
            };

    public Set<Long> forWhomSiteIsDead(long hsid) {
        ImmutableSet.Builder<Long> isb = ImmutableSet.builder();
        Set<Long> witnessedBy = m_witnessed.get(hsid);
        if (witnessedBy != null) {
            isb.addAll(Sets.filter(witnessedBy, amongSurvivors));
        }
        return isb.build();
    }

    protected class Scenario implements Cloneable {

        protected TreeMultimap<Long, Long> reported;
        protected TreeMultimap<Long, Long> witnessed;
        protected Set<Long> survivors;

        protected Scenario() {
            reported = m_reported;
            witnessed = m_witnessed;
            survivors = m_survivors;
        }

        @Override
        protected Scenario clone()  {
            Scenario cloned;
            try {
                cloned = Scenario.class.cast(super.clone());
            } catch (CloneNotSupportedException e) {
                throw new InternalError(e.getMessage());
            }
            cloned.reported = TreeMultimap.create(reported);
            cloned.witnessed = TreeMultimap.create(witnessed);
            cloned.survivors = Sets.newTreeSet(survivors);

            return cloned;
        }
    }
}
