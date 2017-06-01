/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.junit;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.eventbus.DeadEvent;
import com.google_voltpatches.common.eventbus.EventBus;
import com.google_voltpatches.common.eventbus.Subscribe;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private JUnitImporterMessenger() {

    }

    public static JUnitImporterMessenger initialize() {
        if (m_self == null) {
            m_self = new JUnitImporterMessenger();
            m_self.m_eventBus.register(m_self);
        }
        return m_self;
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

    /* ---- EventBus notification subscribers ---- */

    /** Main event tracking mechanism.
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

    /** Allows for tests to safely examine and/or clear the events reported for each importer. */
    public synchronized void checkEventList(JUnitImporterEventExaminer examiner) {
        examiner.examine(m_eventTracker);
    }

    /** Allows access to any errors thrown by importers, and clears the (single entry) buffer where reported exceptions are kept */
    public synchronized Throwable checkForAndClearExceptions() {
        Throwable error = m_firstReportedException;
        m_firstReportedException = null;
        return error;
    }
}
