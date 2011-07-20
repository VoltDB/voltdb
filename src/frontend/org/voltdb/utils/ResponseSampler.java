/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;

public abstract class ResponseSampler {

    private static final VoltLogger LOG = new VoltLogger("LOGGING");

    public static final String ENV_PATH_VAR_NAME = "VOLTDB_RESPONSE_SAMPLE_PATH";
    public static final String SAMPLE_FILE_PREFIX = "voltdb-txn-result-";
    public static final String SAMPLE_FILE_EXTENSION = ".json";
    public static final String REPLAY_TOKEN = "-replay";
    public static final long MAX_SAMPLES = 5000;
    public static final long NON_REPLAY_SAMPLE = 0;

    // local state
    static final SortedSet<Long> m_sampledTxnIds = Collections.synchronizedSortedSet(new TreeSet<Long>());
    static boolean m_sampling = false;
    static File m_sampleDir = null;
    static LinkedBlockingQueue<SamplerWork> m_work = new LinkedBlockingQueue<SamplerWork>(1);
    static Thread m_sampler = null;
    static AtomicBoolean m_shouldContinueSamplerThread = new AtomicBoolean(true);

    // statistics
    public static long prexistingSampleCount = 0;
    public static long totalSamplesTaken = 0;
    public static long errorsFound = 0;
    public static long responsesCompared = 0;

    /**
     * Describes an object queued to be written to disk,
     * but also manages the serialization of the invocation,
     * txnid and response to a common, human readable json
     * format.
     */
    static class SamplerWork {
        final long txnId;
        final String data;

        SamplerWork(long txnId, JSONString invocation, JSONString response) {
            this.txnId = txnId;

            JSONStringer js = new JSONStringer();
            String tempData = "JSON ERROR";
            try {
                js.object();
                js.key("txnid");
                js.value(txnId);
                js.key("invocation");
                js.value(invocation);
                js.key("response");
                js.value(response);
                js.endObject();

                tempData = js.toString();
                JSONObject foo = new JSONObject(tempData);
                tempData = foo.toString(2);
            }
            catch (JSONException e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to serialize an invocation/response pair.", e);
            }

            data = tempData.trim();
        }
    }

    /**
     * Secondary thread that writes samples to disk
     * asyncronously.
     */
    static Runnable m_samplerRunner = new Runnable() {
        @Override
        public void run() {
            LOG.info("Started response sampler processing thread.");

            while (m_shouldContinueSamplerThread.get()) {
                SamplerWork work = null;
                try {
                    work = m_work.poll(250, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {}

                if (work == null)
                    continue;

                // make some room if need be
                while (m_sampledTxnIds.size() >= MAX_SAMPLES) {
                    long oldestTxnId = m_sampledTxnIds.first();
                    File f = txnIdToFile(oldestTxnId, NON_REPLAY_SAMPLE);
                    if (f.exists())
                        f.delete();
                    m_sampledTxnIds.remove(oldestTxnId);
                }

                // write the new file
                writeFile(work.txnId, work.data, NON_REPLAY_SAMPLE);
                m_sampledTxnIds.add(work.txnId);
            }

            LOG.info("Exiting response sampler processing thread.");
        }
    };

    /**
     * Called from RealVoltDB.initialize() to start sampling if the env
     * variable is set.
     */
    public static void initializeIfEnabled() {
        // only start when the env var is present
        if (System.getenv().containsKey(ENV_PATH_VAR_NAME) == false)
            return;

        String sampleDirPath = System.getenv(ENV_PATH_VAR_NAME);
        assert(sampleDirPath != null);

        initialize(sampleDirPath);
    }

    /**
     * Turns on sampling, given a path to sample to. Can be used by tests.
     */
    public static void initialize(String sampleDirPath) {
        // don't initialize twice
        if (m_sampling) return;

        // reset state
        m_sampledTxnIds.clear();
        m_sampling = false;
        m_sampleDir = null;
        m_work.clear();
        m_sampler = null;

        // statistics
        prexistingSampleCount = 0;
        totalSamplesTaken = 0;
        errorsFound = 0;
        responsesCompared = 0;

        LOG.info("Will attempt to sample transaction results to directory: " + sampleDirPath);

        File d = new File(sampleDirPath);
        if (!d.exists()) {
            LOG.error("Transaction result sample directory not found. Will not sample.");
            return;
        }
        if (!d.isDirectory()) {
            LOG.error("Transaction result sample directory is not a directory. Will not sample.");
            return;
        }
        if (!d.canRead() || !d.canWrite()) {
            LOG.error("Transaction result sample directory has restricted permissions. Will not sample.");
            return;
        }
        m_sampleDir = d;

        for (File f : m_sampleDir.listFiles()) {
            String filename = f.getName();
            // skip files that aren't samples
            if (!filename.startsWith(SAMPLE_FILE_PREFIX))
                continue;
            if (filename.contains(REPLAY_TOKEN)) {
                LOG.fatal("Found replay files in the samples directory. Can't continue until cleaned up.");
                VoltDB.crashVoltDB();
            }

            // sanity check
            assert(f.canRead());

            // get the txnid from the filename
            String txnIdStr = filename.substring(
                    SAMPLE_FILE_PREFIX.length(),
                    filename.length() - SAMPLE_FILE_EXTENSION.length());
            long txnId = Long.parseLong(txnIdStr);

            // cache the fact that a sample exists for this txnid
            m_sampledTxnIds.add(txnId);
            ++prexistingSampleCount;
        }

        LOG.info(String.format("Txn response sampler found %d prexisting responses on disk.",
                prexistingSampleCount));

        // start the thread that samples
        m_shouldContinueSamplerThread.set(true);
        m_sampler = new Thread(m_samplerRunner);
        m_sampler.setName("Txn Response Sampler");
        m_sampler.setDaemon(true);
        m_sampler.start();

        m_sampling = true;
    }

    /**
     * Called only by tests, un-initialize..
     */
    public static void uninitializeForTests() {
        assert(m_sampler != null);
        m_shouldContinueSamplerThread.set(false);
        try {
            m_sampler.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert(!m_sampler.isAlive());

        m_sampling = false;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void offerResponse(long initiatorId, long txnId, StoredProcedureInvocation invocation, JSONString responseData) {
        // no-op if not sampling
        if (!m_sampling) return;

        SamplerWork work = null;

        // check if we have a response to compare it to
        if (m_sampledTxnIds.contains(txnId)) {
            work = new SamplerWork(txnId, invocation, responseData);
            checkReplayedResponse(initiatorId, work);
            return;
        }

        // if the txnid is older than any samples, assume this is reply and skip
        if ((m_sampledTxnIds.size() > 0) && (txnId <= m_sampledTxnIds.last())) {
            return;
        }

        // if the sampler is working, don't sample
        if (m_work.remainingCapacity() == 0) {
            return;
        }

        // add the sampler work in a safe way
        work = new SamplerWork(txnId, invocation, responseData);
        try {
            boolean inserted = m_work.add(work);
            assert(inserted);
            ++totalSamplesTaken;
        }
        catch (Exception e) {}
    }

    /**
     * Find the set of files that represent failed matches.
     */
    public static Set<Long> getBadMatchTxnIds() {
        Set<Long> retval = new TreeSet<Long>();

        for (File f : m_sampleDir.listFiles()) {
            String filename = f.getName();
            // skip files that aren't samples
            if (!filename.startsWith(SAMPLE_FILE_PREFIX))
                continue;
            if (!filename.contains(REPLAY_TOKEN))
                continue;

            // sanity check
            assert(f.canRead());

            // get the txnid from the filename
            String txnIdStr = filename.substring(
                    SAMPLE_FILE_PREFIX.length(),
                    filename.length() - (REPLAY_TOKEN.length() + SAMPLE_FILE_EXTENSION.length()));
            long txnId = Long.parseLong(txnIdStr);

            // cache the fact that a sample exists for this txnid
            retval.add(txnId);
        }

        return retval;
    }

    public static boolean printStatsAndReturnFalseIfFailed() {
        System.out.printf("ResponseSampler found %d responses at startup.\n",
                prexistingSampleCount);
        System.out.printf("ResponseSampler compared %d responses during replay with %d bad matches.\n",
                responsesCompared, errorsFound);
        System.out.printf("ResponseSampler added %d new samples (some of which may have been removed).",
                totalSamplesTaken);

        return errorsFound == 0;
    }

    /**
     * Verify that the contents on the disk match the data for the txn we're
     * about to replay.
     * @param initiatorId
     */
    static void checkReplayedResponse(long initiatorId, SamplerWork work) {
        String originalResponse = readFile(work.txnId);
        // silent fail (at least here) if the file can't be found
        if (originalResponse == null)
            return;
        // do the comparison
        if (!originalResponse.equals(work.data)) {
            writeFile(work.txnId, work.data, initiatorId);
            ++errorsFound;
            LOG.error(String.format("Replay response differs for txnId: %d", work.txnId));
        }
        ++responsesCompared;
    }

    /**
     * Given a txn id, return the File object that represents it.
     * Note that the file doesn't have to exist.
     */
    static File txnIdToFile(long txnId, long initiatorId ) {
        String path = String.format("%s%s%s%d%s%s",
                m_sampleDir.getPath(),
                File.separator,
                SAMPLE_FILE_PREFIX,
                txnId,
                initiatorId != NON_REPLAY_SAMPLE ? REPLAY_TOKEN + String.valueOf(initiatorId) : "",
                SAMPLE_FILE_EXTENSION);
        return new File(path);
    }

    /**
     * Given a txn id, look in the magic folder, find
     * the right file, read its contents and return them.
     */
    static String readFile(long txnId) {
        File f = txnIdToFile(txnId, NON_REPLAY_SAMPLE);
        // silent fail if missing
        if (!f.exists())
            return null;
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f),"UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            reader.close();
        } catch (IOException e) {
            String msg = String.format("Unable to read reposnse sample file at path %s", f.getPath());
            LOG.error(msg, e);
            return null;
        }
        String retval = sb.toString();
        assert(retval != null);
        return retval.trim();
    }

    /**
     * Given a txnId, data and whether this is a replayed txn, write the contents
     * to the right filename.
     */
    static void writeFile(long txnId, String jsonResponseData, long initiatorId) {
        File f = txnIdToFile(txnId, initiatorId);
        assert(f.exists() == false);
        assert(f.getParentFile().canWrite());
        try {
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(f),"UTF-8"));
            writer.write(jsonResponseData);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            String msg = String.format("Unable to write reposnse sample file to path %s", f.getPath());
            LOG.error(msg, e);
        }
    }
}

