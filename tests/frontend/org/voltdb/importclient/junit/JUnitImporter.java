/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.junit;

import com.google_voltpatches.common.base.Preconditions;
import org.voltdb.importer.AbstractImporter;

import java.net.URI;
import java.util.List;

import static org.voltdb.importclient.junit.JUnitImporter.Event.*;

/**
 * Created by bshaw on 5/31/17.
 */
public class JUnitImporter extends AbstractImporter {

    public static final int DEFAULT_IMPORTER_SLEEP_DURATION_MS = 250;

    private JUnitImporterConfig m_config;

    public enum Event {
        CONSTRUCTED,
        ACCEPT_CALLED,
        STOP_CALLED,
        NO_LONGER_RUNNING
    }

    public enum State {
        UNKNOWN,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED
    }

    public static State computeStateFromEvents(List<Event> eventList) {
        if (eventList.size() == 0) {
            return State.UNKNOWN; // assume test cleared the event list - this should not be an importer whose constructor isn't done!
        }

        Event mostRecentEvent = eventList.get(eventList.size() - 1);
        Event secondMostRecentEvent = eventList.size() > 1 ? eventList.get(eventList.size() - 2) : null;

        switch (mostRecentEvent) {
            case CONSTRUCTED:
                return State.STARTING;
            case ACCEPT_CALLED:
                return State.RUNNING;
            // There is a race condition between STOP_CALLED and NO_LONGER_RUNNING.
            // Usually STOP_CALLED will be first but don't assume that's always the case.
            case STOP_CALLED:
                if (secondMostRecentEvent == null) {
                    return State.UNKNOWN; // assume test cleared event list
                } else if (secondMostRecentEvent.equals(Event.ACCEPT_CALLED)) {
                    return State.STOPPING;
                } else {
                    assert (secondMostRecentEvent.equals(Event.NO_LONGER_RUNNING) || secondMostRecentEvent.equals(Event.CONSTRUCTED));
                    return State.STOPPED;
                }
            case NO_LONGER_RUNNING:
                if (secondMostRecentEvent == null) {
                    return State.UNKNOWN; // assume test cleared event list
                } else if (secondMostRecentEvent.equals(Event.ACCEPT_CALLED)) {
                    return State.STOPPING;
                } else {
                    assert (secondMostRecentEvent.equals(Event.STOP_CALLED) || secondMostRecentEvent.equals(Event.CONSTRUCTED));
                    return State.STOPPED;
                }
        }
        throw new RuntimeException("We cannot get here but the compiler requires a return statement");
    }


    private JUnitImporterMessenger getMessenger() {
        JUnitImporterMessenger messenger = JUnitImporterMessenger.instance();
        Preconditions.checkState(messenger != null);
        return messenger;
    }

    public JUnitImporter(JUnitImporterConfig config) {
        m_config = config;
        getMessenger().produceEvent(this, CONSTRUCTED);
    }

    @Override
    public String getName() {
        return "JUnitImporter";
    }

    @Override
    public URI getResourceID() {
        return m_config.getResourceID();
    }

    @Override
    protected void accept() {
        info(null, "Test importer started");
        getMessenger().produceEvent(this, ACCEPT_CALLED);
        while (shouldRun()) {
            try {
                Thread.sleep(m_config.getSleepDurationMs());
            } catch (InterruptedException ignoreMe){
            }
        }
        getMessenger().produceEvent(this, NO_LONGER_RUNNING);
        info(null, "Test importer has stopped");
    }

    @Override
    protected void stop() {
        getMessenger().produceEvent(this, STOP_CALLED);
    }
}
