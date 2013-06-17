/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.agreement.maker;

import static com.natpryce.makeiteasy.MakeItEasy.an;
import static com.natpryce.makeiteasy.MakeItEasy.listOf;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static com.natpryce.makeiteasy.Property.newProperty;

import java.util.List;

import org.voltcore.messaging.FailureSiteUpdateMessage;
import org.voltcore.utils.Pair;

import com.google.common.collect.ImmutableMap;
import com.natpryce.makeiteasy.Donor;
import com.natpryce.makeiteasy.Instantiator;
import com.natpryce.makeiteasy.Property;
import com.natpryce.makeiteasy.PropertyLookup;

public class FailureSiteUpdateMessageMaker {

    public final static Property<Pair<Long,Boolean>,Long> pSite = newProperty();
    public final static Property<Pair<Long,Boolean>,Boolean> pWitnessed = newProperty();

    public final static Instantiator<Pair<Long, Boolean>> Entry =
            new Instantiator<Pair<Long,Boolean>>() {
                @Override
                public Pair<Long, Boolean> instantiate(
                        PropertyLookup<Pair<Long, Boolean>> lookup) {
                    return Pair.of(
                            lookup.valueOf(pSite, Long.MIN_VALUE),
                            lookup.valueOf(pWitnessed,Boolean.FALSE)
                            );
                }
            };

    @SuppressWarnings("unchecked")
    public final static Donor<List<Pair<Long,Boolean>>> fsumHsids(
            Long k1, Boolean v1) {
            return listOf(an(Entry,with(pSite,k1),with(pWitnessed,v1)));
    }

    @SuppressWarnings("unchecked")
    public final static Donor<List<Pair<Long,Boolean>>> fsumHsids(
            Long k1, Boolean v1, Long k2, Boolean v2) {
            return listOf(
                    an(Entry,with(pSite,k1),with(pWitnessed,v1)),
                    an(Entry,with(pSite,k2),with(pWitnessed,v2))
                    );
    }

    @SuppressWarnings("unchecked")
    public final static Donor<List<Pair<Long,Boolean>>> fsumHsids(
            Long k1, Boolean v1, Long k2, Boolean v2,
            Long k3, Boolean v3) {
            return listOf(
                    an(Entry,with(pSite,k1),with(pWitnessed,v1)),
                    an(Entry,with(pSite,k2),with(pWitnessed,v2)),
                    an(Entry,with(pSite,k3),with(pWitnessed,v3))
                    );
    }

    @SuppressWarnings("unchecked")
    public final static Donor<List<Pair<Long,Boolean>>> fsumHsids(
            Long k1, Boolean v1, Long k2, Boolean v2,
            Long k3, Boolean v3, Long k4, Boolean v4) {
            return listOf(
                    an(Entry,with(pSite,k1),with(pWitnessed,v1)),
                    an(Entry,with(pSite,k2),with(pWitnessed,v2)),
                    an(Entry,with(pSite,k3),with(pWitnessed,v3)),
                    an(Entry,with(pSite,k4),with(pWitnessed,v4))
                    );
    }

    @SuppressWarnings("unchecked")
    public final static Donor<List<Pair<Long,Boolean>>> fsumHsids(
            Long k1, Boolean v1, Long k2, Boolean v2,
            Long k3, Boolean v3, Long k4, Boolean v4,
            Long k5, Boolean v5) {
            return listOf(
                    an(Entry,with(pSite,k1),with(pWitnessed,v1)),
                    an(Entry,with(pSite,k2),with(pWitnessed,v2)),
                    an(Entry,with(pSite,k3),with(pWitnessed,v3)),
                    an(Entry,with(pSite,k4),with(pWitnessed,v4)),
                    an(Entry,with(pSite,k5),with(pWitnessed,v5))
                    );
    }

    public final static Property<FailureSiteUpdateMessage,Long> fsumSource = newProperty();
    public final static Property<FailureSiteUpdateMessage,Long> fsumSite = newProperty();
    public final static Property<FailureSiteUpdateMessage,Long> fsumTxnid = newProperty();
    public final static Property<FailureSiteUpdateMessage, Iterable<Pair<Long,Boolean>>> fsumEntries = newProperty();

    public final static Instantiator<FailureSiteUpdateMessage> FailureSiteUpdateMessage =
            new Instantiator<FailureSiteUpdateMessage>() {
                @Override
                public FailureSiteUpdateMessage instantiate(
                        PropertyLookup<FailureSiteUpdateMessage> lookup) {
                    Long source = lookup.valueOf(fsumSource, -1L);
                    Long site = lookup.valueOf(fsumSite, -1L);
                    Long txnId = lookup.valueOf(fsumTxnid, -1L);
                    @SuppressWarnings("unchecked")
                    Iterable<Pair<Long,Boolean>> entries = lookup.valueOf(fsumEntries,
                            listOf(an(Entry,with(pSite,site),with(pWitnessed,true))));

                    ImmutableMap.Builder<Long, Boolean> builder = ImmutableMap.builder();
                    for (Pair<Long,Boolean> entry: entries) {
                        builder.put(entry.getFirst(), entry.getSecond());
                    }

                    FailureSiteUpdateMessage instance =
                            new FailureSiteUpdateMessage(
                                    builder.build(),
                                    site,
                                    txnId,
                                    site);
                    instance.m_sourceHSId = source;

                    return instance;
                }
            };
}
