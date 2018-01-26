/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.export;

import java.util.HashMap;
import java.util.Properties;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;

public class ExportTestClient extends ExportClientBase
{
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    // hash table name + partition to verifier
    public static final HashMap<String, ExportTestVerifier> m_verifiers = new HashMap<String, ExportTestVerifier>();
    public static final HashMap<String, Boolean> m_seen_verifiers = new HashMap<String, Boolean>();
    public static TreeSet<Long> m_generationsSeen = new TreeSet<Long>();

    public static Long m_generation;

    public ExportTestClient() {
    }

    @Override
    public void configure(Properties config) throws Exception {
        if (!config.getProperty("complain", "").isEmpty()) {
            throw new IllegalArgumentException("doing what I am being told");
        }
        System.out.println("Test Export Client configured onserver with properties: " + config);
    }

    public void setVerifierGeneration(Long gen) {
        for (String sverifier : m_verifiers.keySet()) {
            boolean genSeen = false;
            for (Long generation : m_generationsSeen) {
                if (sverifier.contains(generation.toString())) {
                    genSeen = true;
                    break;
                }
            }
            if (!genSeen) {
                ExportTestVerifier verifier = m_verifiers.get(sverifier);
                m_verifiers.remove(sverifier);
                m_verifiers.put((sverifier + "-" + gen), verifier);
            }
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        synchronized(ExportTestClient.class) {
            if (m_generation == null) {
                setVerifierGeneration(source.m_generation);
            }
            m_generation = source.m_generation;
            m_generationsSeen.add(source.m_generation);

            String key = source.tableName + "-" + source.partitionId + "-" + m_generation;
            if (m_verifiers.containsKey(key)) {
                m_seen_verifiers.put(key, Boolean.TRUE);
                return m_verifiers.get(key);
            }

            // create a verifier with the 'schema'
            ExportTestVerifier verifier = new ExportTestVerifier(source);
            // hash it by table name + partition ID
            m_logger.info("Creating verifier for table: " + source.tableName +
                    ", part ID: " + source.partitionId);
            System.out.println("Creating verifier for table: " + source.tableName
                    + ", part ID: " + source.partitionId);
            m_verifiers.put(key, verifier);
            m_seen_verifiers.put(key, Boolean.TRUE);
            return verifier;
        }
    }

    public synchronized static void addRow(Client client, String tableName, Object partitionHash, Object[] data) throws Exception {
        long partition = ((ClientImpl) client).getPartitionForParameter(VoltType.typeFromObject(partitionHash).getValue(),
                partitionHash);

        String key;
        if (m_generation == null)
            key = tableName + "-" + partition;
        else
            key = tableName + "-" + partition + "-" + m_generation;
        ExportTestVerifier verifier = m_verifiers.get(key);
        if (verifier == null) {
            System.out.println("No verifier for table " + tableName + " and partition " + partition);
            System.out.println("Expected Verifiers registered: " + m_verifiers);
            AdvertisedDataSource source = new AdvertisedDataSource((int) partition, "sign" + partition,
                    tableName, "", 0, 0, null, null, null, ExportFormat.FOURDOTFOUR);
            verifier = new ExportTestVerifier(source);
            m_verifiers.put(key, verifier);
        }
        verifier.addRow(data);
    }

    public static synchronized void clear() {
        m_seen_verifiers.clear();
        m_verifiers.clear();
        m_generationsSeen.clear();
        m_generation = null;
    }

    public static boolean allRowsVerified()
    {
        boolean retval = true;
        for (String sverifier : m_verifiers.keySet()) {
            if (m_seen_verifiers.containsKey(sverifier) && sverifier.contains(m_generation.toString())) {
                System.out.println("Seen: " + sverifier);
                ExportTestVerifier verifier = m_verifiers.get(sverifier);
                if (!verifier.allRowsVerified()) {
                    System.out.println("Verifier Failed: " + verifier.getSize());
                    retval = false;
                }
            }
        }
        clear();
        return retval;
    }

    public static long getExportedDataCount() {
        long retval = 0;
        for (ExportTestVerifier verifier : m_verifiers.values()) {
            retval += verifier.getSize();
        }
        return retval;
    }

    @Override
    public void shutdown() {
    }

}
