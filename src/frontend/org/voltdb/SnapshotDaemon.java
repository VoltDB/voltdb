/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.apache.log4j.Logger;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.utils.Pair;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;
import java.io.File;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * A scheduler of automated snapshots and manager of archived and retained snapshots.
 *
 */
class SnapshotDaemon {

    private static final Logger hostLog =
        Logger.getLogger("HOST", org.voltdb.utils.VoltLoggerFactory.instance());

    private final TimeUnit m_frequencyUnit;
    private final long m_frequencyInMillis;
    private final int m_frequency;
    private final int m_retain;
    private final String m_path;
    private final String m_prefix;
    private final String m_prefixAndSeparator;

    private final SimpleDateFormat m_dateFormat = new SimpleDateFormat("'_'yyyy.MM.dd.HH.mm.ss");

    // true if this SnapshotDaemon is the one responsible for generating
    // snapshots
    private boolean m_isActive = false;
    private long m_nextSnapshotTime = System.currentTimeMillis();

    /**
     * Don't invoke sysprocs too close together.
     * Keep track of the last call and only do it after
     * enough time has passed.
     */
    private long m_lastSysprocInvocation = System.currentTimeMillis();
    private final long m_minTimeBetweenSysprocs = 7000;

    /**
     * List of snapshots on disk sorted by creation time
     */
    final LinkedList<Snapshot> m_snapshots = new LinkedList<Snapshot>();

    /**
     * States the daemon can be in
     *
     */
    enum State {
        /*
         * Initial state
         */
        STARTUP,
        /*
         * Invoked @SnapshotScan, waiting for results.
         * Done once on startup to find number of snapshots on disk
         * at path with prefix
         */
        SCANNING,
        /*
         * Waiting in between snapshots
         */
        WAITING,
        /*
         * Deleting snapshots that are no longer going to be retained.
         */
        DELETING,
        /*
         * Initiated a snapshot. Will call snapshot scan occasionally to find out
         * when it completes.
         */
        SNAPSHOTTING,
        /*
         * Failure state. This state is entered when a sysproc
         * fails and the snapshot Daemon can't recover. An error is logged
         * and the Daemon stops working
         */
        FAILURE;
    }

    private State m_state = State.STARTUP;

    SnapshotDaemon(final SnapshotSchedule schedule) {
        if (schedule == null) {
            m_frequencyUnit = null;
            m_retain = 0;
            m_frequency = 0;
            m_frequencyInMillis = 0;
            m_prefix = null;
            m_path = null;
            m_prefixAndSeparator = null;
        } else {
            m_frequency = schedule.getFrequencyvalue();
            m_retain = schedule.getRetain();
            m_path = schedule.getPath();
            m_prefix = schedule.getPrefix();
            m_prefixAndSeparator = m_prefix + "_";
            final String frequencyUnitString = schedule.getFrequencyunit().toLowerCase();
            assert(frequencyUnitString.length() == 1);
            final char frequencyUnit = frequencyUnitString.charAt(0);

            switch (frequencyUnit) {
            case 's':
                m_frequencyUnit = TimeUnit.SECONDS;
                break;
            case 'm':
                m_frequencyUnit = TimeUnit.MINUTES;
                break;
            case 'h':
                m_frequencyUnit = TimeUnit.HOURS;
                break;
                default:
                    throw new RuntimeException("Frequency unit " + frequencyUnitString + "" +
                            " in snapshot schedule is not one of d,m,h");
            }
            m_frequencyInMillis = TimeUnit.MILLISECONDS.convert( m_frequency, m_frequencyUnit);
            m_nextSnapshotTime += m_frequencyInMillis;
        }
    }

    /**
     * Make this SnapshotDaemon responsible for generating snapshots
     */
    public void makeActive()
    {
        m_isActive = true;
    }

    private class Snapshot implements Comparable<Snapshot> {
        private final String path;
        private final String nonce;
        private final Long created;

        private Snapshot (String path, String nonce, Long created) {
            this.path = path;
            this.nonce = nonce;
            this.created = created;
        }

        @Override
        public int compareTo(Snapshot o) {
            return created.compareTo(o.created);
        }
    }

    /**
     * Invoked by the client interface occasionally. Returns null
     * if nothing needs to be done or the name of a sysproc along with procedure parameters
     * if there is work to be done. Responses come back later via invocations
     * of processClientResponse
     * @param now Current time
     * @return null if there is no work to do or a sysproc with parameters if there is work
     */
    synchronized Pair<String, Object[]> processPeriodicWork(final long now) {
        if (!m_isActive)
        {
            return null;
        }

        if (m_frequencyUnit == null) {
            return null;
        }

        if (m_state == State.STARTUP) {
            return initiateSnapshotScan();
        } else if (m_state == State.SCANNING) {
            return null;
        } else if (m_state == State.FAILURE) {
            return null;
        } else if (m_state == State.WAITING){
            return processWaitingPeriodicWork(now);
        } else if (m_state == State.SNAPSHOTTING) {
            return null;
        } else if (m_state == State.DELETING){
            return null;
        }

        return null;
    }

    /**
     * Do periodic work when the daemon is in the waiting state. The
     * daemon paces out sysproc invocations over time
     * to avoid disrupting regular work. If the time for the next
     * snapshot has passed it attempts to initiate a new snapshot.
     * If there are too many snapshots being retains it attempts to delete
     * the extras. Then it attempts to initiate a new snapshot if
     * one is due
     */
    private Pair<String, Object[]> processWaitingPeriodicWork(long now) {
        if (now - m_lastSysprocInvocation < m_minTimeBetweenSysprocs) {
            return null;
        }

        if (m_snapshots.size() > m_retain) {
            return deleteExtraSnapshots();
        }

        if (m_nextSnapshotTime < now) {
            return initiateNextSnapshot(now);
        }

        return null;
    }

    private Pair<String, Object[]> initiateNextSnapshot(long now) {
        m_state = State.SNAPSHOTTING;
        m_lastSysprocInvocation = now;
        final Date nowDate = new Date(now);
        final String dateString = m_dateFormat.format(nowDate);
        final String nonce = m_prefix + dateString;
        Object params[] = new Object[3];
        params[0] = m_path;
        params[1] = nonce;
        params[2] = 0;//don't block
        m_snapshots.offer(new Snapshot(m_path, nonce, now));
        return Pair.of("@SnapshotSave", params);
    }

    /**
     * Invoke the \@SnapshotScan system procedure to discover
     * snapshots on disk that are managed by this daemon
     * @return
     */
    private Pair<String, Object[]> initiateSnapshotScan() {
        m_lastSysprocInvocation = System.currentTimeMillis();
        Object params[] = new Object[1];
        params[0] = m_path;
        m_state = State.SCANNING;
        return Pair.of("@SnapshotScan", params);
    }

    /**
     * Process responses to sysproc invocations generated by this daemon
     * via processPeriodicWork
     * @param response
     * @return
     */
    synchronized Pair<String, Object[]> processClientResponse(ClientResponse response) {
        if (m_frequencyUnit == null) {
            throw new RuntimeException("SnapshotDaemon received a response when it has not been configured to run");
        }

        if (m_state == State.STARTUP) {
            throw new RuntimeException("SnapshotDaemon received a response in the startup state");
        } else if (m_state == State.SCANNING) {
            return processScanResponse(response);
        } else if (m_state == State.FAILURE) {
            return null;
        } else if (m_state == State.DELETING){
            processDeleteResponse(response);
            return null;
        } else if (m_state == State.SNAPSHOTTING){
            processSnapshotResponse(response);
            return null;
        }

        return null;
    }

    /**
     * Confirm and log that the snapshot was a success
     * @param response
     */
    private void processSnapshotResponse(ClientResponse response) {
        m_state = State.WAITING;
        final long now = System.currentTimeMillis();
        m_nextSnapshotTime += m_frequencyInMillis;
        if (m_nextSnapshotTime < now) {
            m_nextSnapshotTime = now - 1;
        }

        if (response.getStatus() != ClientResponse.SUCCESS){
            m_state = State.FAILURE;
            logFailureResponse("Snapshot failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        final VoltTable result = results[0];

        if (result.getColumnCount() == 1) {
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Snapshot failed with failure response: " + result.getString(0));
            return;
        }

        //assert(result.getColumnName(1).equals("TABLE"));

        while (result.advanceRow()) {
            if (!result.getString("RESULT").equals("SUCCESS")) {
                hostLog.error("Snapshot save feasability test failed for host "
                        + result.getString("HOST_ID") + " table " + result.getString("TABLE") +
                        " with error message " + result.getString("ERR_MSG"));
            }
        }
    }

    /**
     * Process a response to a request to delete snapshots.
     * Always transitions to the waiting state even if the delete
     * fails. This ensures the system will continue to snapshot
     * until the disk is full in the event that there is an administration
     * error or a bug.
     * @param response
     */
    private void processDeleteResponse(ClientResponse response) {
        //Continue snapshotting even if a delete fails.
        m_state = State.WAITING;
        if (response.getStatus() != ClientResponse.SUCCESS){
            /*
             * The delete may fail but the procedure should at least return success...
             */
            m_state = State.FAILURE;
            logFailureResponse("Delete of snapshots failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        assert(results.length > 0);
        if (results[0].getColumnCount() == 1) {
            final VoltTable result = results[0];
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Snapshot delete failed with failure response: " + result.getString("ERR_MSG"));
            return;
        }
    }

    /**
     * Process the response to a snapshot scan. Find the snapshots
     * that are managed by this daemon by path and nonce
     * and add it the list. Initiate a delete of any that should
     * not be retained
     * @param response
     * @return
     */
    private Pair<String, Object[]> processScanResponse(ClientResponse response) {
        if (response.getStatus() != ClientResponse.SUCCESS){
            m_state = State.FAILURE;
            logFailureResponse("Initial snapshot scan failed", response);
            return null;
        }

        final VoltTable results[] = response.getResults();
        if (results.length == 1) {
            m_state = State.FAILURE;
            final VoltTable result = results[0];
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Initial snapshot scan failed with failure response: " + result.getString("ERR_MSG"));
            return null;
        }
        assert(results.length == 3);

        final VoltTable snapshots = results[0];
        assert(snapshots.getColumnCount() == 8);

        final File myPath = new File(m_path);
        while (snapshots.advanceRow()) {
            final String path = snapshots.getString("PATH");
            final File pathFile = new File(path);
            if (pathFile.equals(myPath)) {
                final String nonce = snapshots.getString("NONCE");
                if (nonce.startsWith(m_prefixAndSeparator)) {
                    final Long created = snapshots.getLong("CREATED");
                    m_snapshots.add(new Snapshot(path, nonce, created));
                }
            }
        }

        java.util.Collections.sort(m_snapshots);

        return deleteExtraSnapshots();
    }

    /**
     * Check if there are extra snapshots and initate deletion
     * @return
     */
    private Pair<String, Object[]> deleteExtraSnapshots() {
        if (m_snapshots.size() <= m_retain) {
            m_state = State.WAITING;
        } else {
            m_lastSysprocInvocation = System.currentTimeMillis();
            m_state = State.DELETING;
            final int numberToDelete = m_snapshots.size() - m_retain;
            String pathsToDelete[] = new String[numberToDelete];
            String noncesToDelete[] = new String[numberToDelete];
            for (int ii = 0; ii < numberToDelete; ii++) {
                final Snapshot s = m_snapshots.poll();
                pathsToDelete[ii] = s.path;
                noncesToDelete[ii] = s.nonce;
                hostLog.info("Snapshot daemon deleting " + s.nonce);
            }
            Object params[] =
                new Object[] {
                    pathsToDelete,
                    noncesToDelete,
                    };
            return Pair.of("@SnapshotDelete", params);
        }
        return null;
    }

    private void logFailureResponse(String message, ClientResponse response) {
        hostLog.error(message, response.getException());
        if (response.getStatusString() != null) {
            hostLog.error(response.getStatusString());
        }
    }

    State getState() {
        return m_state;
    }

}
