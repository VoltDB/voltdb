/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltcore.agreement.matcher;

import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.primitives.Longs;
import com.natpryce.makeiteasy.Donor;

public class SiteFailureMatchers {

    static public final Matcher<SiteFailureMessage> siteFailureIs(
            final Donor<List<Pair<Long,Long>>> safeTxns, final long...survivors) {

        final Map<Long,Long> safeTxnIds = Maps.newHashMap();
        for (Pair<Long,Long> sp: safeTxns.value()) {
            safeTxnIds.put(sp.getFirst(),sp.getSecond());
        }
        final Set<Long> survivorSet = ImmutableSet.copyOf(Longs.asList(survivors));

        return new TypeSafeMatcher<SiteFailureMessage>() {

            @Override
            public void describeTo(Description d) {
                d.appendText("SiteFailureMessage [")
                .appendText("survivors: ").appendValueList("", ", ", "", Longs.asList(survivors))
                .appendText("safeTxnIds: ").appendValue(safeTxnIds)
                .appendText("]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureMessage m) {
                return equalTo(survivorSet).matches(m.m_survivors)
                    && equalTo(safeTxnIds).matches(m.m_safeTxnIds);
            }
        };
    }

    static public final Matcher<SiteFailureMessage> siteFailureIs(
            final Donor<List<Pair<Long,Long>>> safeTxns,
            final Set<Long> failures,
            final Set<Long> survivors) {

        final Map<Long,Long> safeTxnIds = Maps.newHashMap();
        for (Pair<Long,Long> sp: safeTxns.value()) {
            safeTxnIds.put(sp.getFirst(),sp.getSecond());
        }

        return new TypeSafeMatcher<SiteFailureMessage>() {

            @Override
            public void describeTo(Description d) {
                d.appendText("SiteFailureMessage [")
                .appendText("failed: ").appendValueList("", ", ", "", failures)
                .appendText(", survivors: ").appendValueList("", ", ", "", survivors)
                .appendText(", safeTxnIds: ").appendValue(safeTxnIds)
                .appendText("]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureMessage m) {
                return equalTo(survivors).matches(m.m_survivors)
                    && equalTo(failures).matches(m.m_failed)
                    && equalTo(safeTxnIds).matches(m.m_safeTxnIds);
            }
        };
    }

    static public final Matcher<SiteFailureMessage> siteFailureIs(
            final Set<Long> failures,
            final Set<Long> survivors) {

        return new TypeSafeMatcher<SiteFailureMessage>() {

            @Override
            public void describeTo(Description d) {
                d.appendText("SiteFailureMessage [")
                .appendText("failed: ").appendValueList("", ", ", "", failures)
                .appendText(", survivors: ").appendValueList("", ", ", "", survivors)
                .appendText("]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureMessage m) {
                return equalTo(failures).matches(m.m_failed)
                    && equalTo(survivors).matches(m.m_survivors);
            }
        };
    }

    static public final Matcher<SiteFailureMessage> siteFailureIs(
            final Donor<List<Pair<Long,Long>>> safeTxns,
            final Set<Long> decision,
            final long...survivors) {

        final Map<Long,Long> safeTxnIds = Maps.newHashMap();
        for (Pair<Long,Long> sp: safeTxns.value()) {
            safeTxnIds.put(sp.getFirst(),sp.getSecond());
        }
        final Set<Long> survivorSet = ImmutableSet.copyOf(Longs.asList(survivors));

        return new TypeSafeMatcher<SiteFailureMessage>() {

            @Override
            public void describeTo(Description d) {
                d.appendText("SiteFailureMessage [")
                .appendText("decision: ").appendValueList("", ", ", "", decision)
                .appendText(", survivors: ").appendValueList("", ", ", "", Longs.asList(survivors))
                .appendText(", safeTxnIds: ").appendValue(safeTxnIds)
                .appendText("]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureMessage m) {
                return equalTo(survivorSet).matches(m.m_survivors)
                    && equalTo(decision).matches(m.m_decision)
                    && equalTo(safeTxnIds).matches(m.m_safeTxnIds);
            }
        };
    }

    static public final Matcher<SiteFailureForwardMessage> failureForwardMsgIs(
            final long reportingHsid,
            final Donor<List<Pair<Long,Long>>> safeTxns, final long...survivors) {

        return new TypeSafeMatcher<SiteFailureForwardMessage>() {

            final Matcher<SiteFailureMessage> sfmIs =
                    siteFailureIs(safeTxns,survivors);

            @Override
            public void describeTo(Description d) {
                d.appendText("FailureSiteForwardMessage [ ")
                .appendText("reportingSite: ").appendValue(reportingHsid)
                .appendText(", ").appendDescriptionOf(sfmIs)
                .appendText(" ]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureForwardMessage m) {
                return sfmIs.matches(m)
                    && equalTo(reportingHsid).matches(m.m_reportingHSId);
            }
        };
    }

    static public final Matcher<SiteFailureForwardMessage> failureForwardMsgIs(
            final long reportingHsid,
            final Donor<List<Pair<Long,Long>>> safeTxns,
            final Set<Long> failures,
            final Set<Long> survivors) {

        return new TypeSafeMatcher<SiteFailureForwardMessage>() {

            final Matcher<SiteFailureMessage> sfmIs =
                    siteFailureIs(safeTxns,failures,survivors);

            @Override
            public void describeTo(Description d) {
                d.appendText("FailureSiteForwardMessage [ ")
                .appendText("reportingSite: ").appendValue(reportingHsid)
                .appendText(", ").appendDescriptionOf(sfmIs)
                .appendText(" ]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureForwardMessage m) {
                return sfmIs.matches(m)
                    && equalTo(reportingHsid).matches(m.m_reportingHSId);
            }
        };
    }

    static public final Matcher<SiteFailureForwardMessage> failureForwardMsgIs(
            final long reportingHsid,
            final Set<Long> failures,
            final Set<Long> survivors) {

        return new TypeSafeMatcher<SiteFailureForwardMessage>() {

            final Matcher<SiteFailureMessage> sfmIs =
                    siteFailureIs(failures,survivors);

            @Override
            public void describeTo(Description d) {
                d.appendText("FailureSiteForwardMessage [ ")
                .appendText("reportingSite: ").appendValue(reportingHsid)
                .appendText(", ").appendDescriptionOf(sfmIs)
                .appendText(" ]");
            }

            @Override
            protected boolean matchesSafely(SiteFailureForwardMessage m) {
                return sfmIs.matches(m)
                    && equalTo(reportingHsid).matches(m.m_reportingHSId);
            }
        };
    }
}
