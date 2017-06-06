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
import com.google_voltpatches.common.eventbus.DeadEvent;
import com.google_voltpatches.common.eventbus.EventBus;
import com.google_voltpatches.common.eventbus.Subscribe;

import java.net.URI;
import java.util.*;

/**
 * Created by bshaw on 5/31/17.
 */
public class JUnitImporterMessenger {

    private static JUnitImporterMessenger m_self = null;
    private EventBus m_eventBus = new EventBus("JUnitImporterMessenger");
    private Map<URI, List<JUnitImporter.Event>> m_eventTracker = new HashMap<>();
    private Throwable m_firstReportedException = null;

    private static class Event {
        private URI                 m_importerID;
        private JUnitImporter.Event m_eventType;

        public Event(URI importerID, JUnitImporter.Event eventType) {
            m_importerID = importerID;
            m_eventType = eventType;
        }

        public URI getImporterID() {
            return m_importerID;
        }

        public JUnitImporter.Event getEventType() {
            return m_eventType;
        }
    }

    public static JUnitImporterMessenger initialize() {
        if (m_self == null) {
            m_self = new JUnitImporterMessenger();
            m_self.m_eventBus.register(m_self);
        }
        return m_self;
    }

    public static void deinitialize() {
        m_self = null;
    }

    public static JUnitImporterMessenger instance() {
        Preconditions.checkState(m_self != null, "JUnitImporterMessenger is not initialized");
        return m_self;
    }

    /** Called by importer to inform the test that something interesting happened.
     * @param importer
     * @param eventType
     */
    public void produceEvent(JUnitImporter importer, JUnitImporter.Event eventType) {
        m_eventBus.post(new Event(importer.getResourceID(), eventType));
    }

    public void reportException(Throwable t) {
        m_eventBus.post(t);
    }

    /* ---- Methods for checking importer states from a test ---- */

    public synchronized Set<URI> getRunningImporters(Set<URI> importers, boolean includeTransitioningImporters) {
        importers.clear();
        for (Map.Entry<URI, List<JUnitImporter.Event>> entry : m_eventTracker.entrySet()) {
            boolean includeImporter;
            switch (JUnitImporter.computeCurrentState(entry.getValue())) {
                case RUNNING:
                    includeImporter = true;
                    break;
                case STOPPING:
                case STARTING:
                    includeImporter = includeTransitioningImporters;
                    break;
                case STOPPED:
                    includeImporter = false;
                    break;
                default:
                    throw new IllegalStateException("Importer " + entry.getKey() + " state is unknown");
            }
            if (includeImporter) {
                importers.add(entry.getKey());
            }
        }
        return importers;
    }

    public Map<URI,Integer> countRestarts() {
        Map<URI, Integer> restartMap = new HashMap<>();
        synchronized (this) {
            for (Map.Entry<URI, List<JUnitImporter.Event>> entry : m_eventTracker.entrySet()) {
                restartMap.put(entry.getKey(), JUnitImporter.computeRestartCount(entry.getValue()));
            }
        }
        return restartMap;
    }

    /* ---- EventBus notification subscribers ---- */

    /** Main event tracking mechanism.
     * @param stateChangeEvent
     */
    @Subscribe
    public synchronized void handleImporterStateChange(Event stateChangeEvent) {
        List<JUnitImporter.Event> eventList = m_eventTracker.get(stateChangeEvent.getImporterID());
        if (eventList == null) {
            eventList = new ArrayList<>();
            m_eventTracker.put(stateChangeEvent.getImporterID(), eventList);
        }
        eventList.add(stateChangeEvent.getEventType());
    }

    /** Reports an exception. Only the first one is logged.
     * This mechanism is for ensuring tests fail, not for reliably reporting all errors.
     */
    @Subscribe
    public synchronized void handleExceptionReport(Throwable throwable) {
        if (m_firstReportedException == null) {
            m_firstReportedException = throwable;
        }
    }

    /** We don't expect to see any dead events, so report them as errors. */
    @Subscribe
    public synchronized void handleDeadEvent(DeadEvent deadEvent) {
        handleExceptionReport(new IllegalStateException("Got event with no subscribers with message: " + deadEvent.getEvent().toString()));
    }

    /** Allows access to any errors thrown by importers, and clears the (single entry) buffer where reported exceptions are kept */
    public synchronized Throwable checkForAndClearExceptions() {
        Throwable error = m_firstReportedException;
        m_firstReportedException = null;
        return error;
    }
}
