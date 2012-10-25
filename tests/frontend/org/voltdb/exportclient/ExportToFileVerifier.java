/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltdb.exportclient;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.export.ExportTestVerifier;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class ExportToFileVerifier {
    // hash table name + partition to verifier
    public final TreeMap<Long, HashMap<String, ExportTestVerifier>> m_verifiers =
        new TreeMap<Long, HashMap<String, ExportTestVerifier>>();

    public TreeSet<Long> m_generationsSeen = new TreeSet<Long>();

    private final File m_paths[];
    private final String m_nonce;

    public ExportToFileVerifier(File paths[], String nonce) {
        m_paths = paths;
        m_nonce = nonce;
    }

    /*
     * Find out what the current generation is by loading all the visible generations into a tree map.
     * The highest value will be the current generation ID.
     */
    public void loadGenerations() {
        for (File f : m_paths) {
            System.out.println("Loading path " + f);
            for (File f2 : f.listFiles()) {
                System.out.println("Loading subpath " + f2);
                if (f2.getName().endsWith("csv")) {
                    if (f2.getName().startsWith("active")) {
                        m_generationsSeen.add(Long.valueOf(f2.getName().split("-")[2]));
                    } else {
                        m_generationsSeen.add(Long.valueOf(f2.getName().split("-")[1]));
                    }
                }
            }
        }
    }

    public void addRow(Long generation, String tableName, Object partitionHash, Object[] data)
    {
        int partition = TheHashinator.hashToPartition(partitionHash);
        HashMap<String, ExportTestVerifier> verifiers = m_verifiers.get(generation);
        if (verifiers == null) {
            verifiers = new HashMap<String, ExportTestVerifier>();
            m_verifiers.put( generation, verifiers);
        }

        ExportTestVerifier verifier = verifiers.get(tableName + partition);
        if (verifier == null)
        {
            // something horribly wrong, bail
            System.out.println("No verifier for table " + tableName + " and partition " + partition);
            System.exit(1);
        }
        //verifier.addRow(data);
    }

    public boolean verifyRows() throws Exception {
        for (File f : m_paths) {
            for (File f2 : f.listFiles()) {
                if (f2.getName().endsWith("csv")) {
                    Long generation;
                    String tableName;
                    if (f2.getName().startsWith("active")) {
                        generation = Long.valueOf(f2.getName().split("-")[2]);
                        tableName = f2.getName().split("-")[1];
                    } else {
                        generation = Long.valueOf(f2.getName().split("-")[1]);
                        tableName = f2.getName().split("-")[0];
                    }

                    FileInputStream fis = new FileInputStream(f2);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    InputStreamReader isr = new InputStreamReader(bis, VoltDB.UTF8ENCODING);
                    CSVReader csvreader = new CSVReader(isr);
                    String next[] = null;

                    HashMap<String, ExportTestVerifier> tableToVerifier = m_verifiers.get(generation);
                    if (tableToVerifier == null) {
                        System.out.println("No verifiers at all for generation for generation " + generation);
                    }

                    while ((next = csvreader.readNext()) != null) {
                        StringBuilder sb = new StringBuilder();
                        for (String s : next) {
                            sb.append(s).append(", ");
                        }
                    }
                }
            }
        }
        return false;
    }
}
