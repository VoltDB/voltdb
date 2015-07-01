/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.listOf;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static com.natpryce.makeiteasy.Property.newProperty;

import java.util.List;
import java.util.Set;

import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.primitives.Longs;
import com.natpryce.makeiteasy.Donor;
import com.natpryce.makeiteasy.Instantiator;
import com.natpryce.makeiteasy.Property;
import com.natpryce.makeiteasy.PropertyLookup;

public class SiteFailureMessageMaker {

    public final static Property<Pair<Long, ?>,Long> pSite = newProperty();
    public final static Property<Pair<Long,Long>,Long> pSafe = newProperty();

    public final static Instantiator<Pair<Long,Long>> safePair =
            new Instantiator<Pair<Long,Long>>() {
                @Override
                public Pair<Long, Long> instantiate(
                        PropertyLookup<Pair<Long, Long>> lookup) {
                    return Pair.of(
                            lookup.valueOf(pSite, Long.MIN_VALUE),
                            lookup.valueOf(pSafe, Long.MIN_VALUE)
                            );
                }
            };

    @SuppressWarnings("unchecked")
    public static Donor<List<Pair<Long,Long>>> sfmSafe(long h1, long s1) {
        return listOf(
                a(safePair,with(pSite,h1),with(pSafe,s1))
                );
    }

    @SuppressWarnings("unchecked")
    public static Donor<List<Pair<Long,Long>>> sfmSafe(
            long h1, long s1, long h2, long s2) {
        return listOf(
                a(safePair,with(pSite,h1),with(pSafe,s1)),
                a(safePair,with(pSite,h2),with(pSafe,s2))
                );
    }

    @SuppressWarnings("unchecked")
    public static Donor<List<Pair<Long,Long>>> sfmSafe(
            long h1, long s1, long h2, long s2,
            long h3, long s3) {
        return listOf(
                a(safePair,with(pSite,h1),with(pSafe,s1)),
                a(safePair,with(pSite,h2),with(pSafe,s2)),
                a(safePair,with(pSite,h3),with(pSafe,s3))
                );
    }

    @SuppressWarnings("unchecked")
    public static Donor<List<Pair<Long,Long>>> sfmSafe(
            long h1, long s1, long h2, long s2,
            long h3, long s3, long h4, long s4) {
        return listOf(
                a(safePair,with(pSite,h1),with(pSafe,s1)),
                a(safePair,with(pSite,h2),with(pSafe,s2)),
                a(safePair,with(pSite,h3),with(pSafe,s3)),
                a(safePair,with(pSite,h4),with(pSafe,s4))
                );
    }

    public static Set<Long> sfmFailed(long...vals) {
        return ImmutableSet.copyOf(Longs.asList(vals));
    }

    public static Set<Long> sfmSurvived(long...vals) {
        return ImmutableSet.copyOf(Longs.asList(vals));
    }

    public static Set<Long> sfmDecided(long...vals) {
        return ImmutableSet.copyOf(Longs.asList(vals));
    }

    public static final Property<SiteFailureMessage,Long> sfmSource = newProperty();
    public static final Property<SiteFailureMessage, Iterable<Long>> sfmFailures = newProperty();
    public static final Property<SiteFailureMessage, Iterable<Long>> sfmDecision = newProperty();
    public static final Property<SiteFailureMessage, Iterable<Long>> sfmSurvivors = newProperty();
    public static final Property<SiteFailureMessage, Iterable<Pair<Long,Long>>> sfmSafeTxns = newProperty();

    public final static Instantiator<SiteFailureMessage> SiteFailureMessage =
            new Instantiator<SiteFailureMessage>() {

               @Override
               @SuppressWarnings("unchecked")
               public SiteFailureMessage instantiate(PropertyLookup<SiteFailureMessage> lookup) {

                   SiteFailureMessage.Builder builder =  new SiteFailureMessage.Builder();

                   builder.survivors(Sets.newHashSet(lookup.valueOf(sfmSurvivors, Longs.asList(1))));
                   builder.failures(Sets.newHashSet(lookup.valueOf(sfmFailures, Longs.asList(1))));
                   builder.decisions(Sets.newHashSet(lookup.valueOf(sfmDecision, Longs.asList(1))));

                   for (Pair<Long,Long> sp: lookup.valueOf(sfmSafeTxns, listOf(a(safePair)))) {
                       builder.safeTxnId(sp.getFirst(), sp.getSecond());
                   }
                   SiteFailureMessage msg = builder.build();

                   long source = lookup.valueOf(sfmSource,1L);
                   msg.m_sourceHSId = source;

                   return msg;
                }
            };

    public static final Property<SiteFailureForwardMessage,Long> fsfmSource = newProperty();
    public static final Property<SiteFailureForwardMessage,SiteFailureMessage> fsfmMsg = newProperty();

    public final static Instantiator<SiteFailureForwardMessage> FailureSiteForwardMessage =
            new Instantiator<SiteFailureForwardMessage>() {
                @Override
                public SiteFailureForwardMessage instantiate(
                        PropertyLookup<SiteFailureForwardMessage> lookup) {
                    @SuppressWarnings("unchecked")
                    SiteFailureMessage msg = lookup.valueOf(fsfmMsg, make(a(SiteFailureMessage)));
                    SiteFailureForwardMessage fwd = new SiteFailureForwardMessage(msg);
                    fwd.m_sourceHSId = lookup.valueOf(fsfmSource,1L);
                    return fwd;
                }
            };

}
