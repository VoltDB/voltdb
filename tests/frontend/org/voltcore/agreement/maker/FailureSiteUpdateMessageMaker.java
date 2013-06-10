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

import static com.natpryce.makeiteasy.Property.newProperty;

import org.voltcore.messaging.FailureSiteUpdateMessage;

import com.google.common.collect.ImmutableSet;
import com.natpryce.makeiteasy.Instantiator;
import com.natpryce.makeiteasy.Property;
import com.natpryce.makeiteasy.PropertyLookup;

public class FailureSiteUpdateMessageMaker {

    public final static Property<FailureSiteUpdateMessage,Long> fsumSource = newProperty();
    public final static Property<FailureSiteUpdateMessage,Long> fsumSite = newProperty();
    public final static Property<FailureSiteUpdateMessage,Long> fsumTxnid = newProperty();

    public final static Instantiator<FailureSiteUpdateMessage> FailureSiteUpdateMessage =
            new Instantiator<FailureSiteUpdateMessage>() {
                @Override
                public FailureSiteUpdateMessage instantiate(
                        PropertyLookup<FailureSiteUpdateMessage> lookup) {
                    Long source = lookup.valueOf(fsumSource, -1L);
                    Long site = lookup.valueOf(fsumSite, -1L);
                    Long txnId = lookup.valueOf(fsumTxnid, -1L);

                    FailureSiteUpdateMessage instance =
                            new FailureSiteUpdateMessage(ImmutableSet.<Long>of(), site, txnId, site);
                    instance.m_sourceHSId = source;

                    return instance;
                }
            };
}
