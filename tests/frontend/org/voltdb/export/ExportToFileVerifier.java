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

package org.voltdb.export;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;

import au.com.bytecode.opencsv_voltpatches.CSVReader;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;

public class ExportToFileVerifier {
    // hash table name + partition to verifier
    public final HashMap<String, ExportToFileTestVerifier> m_verifiers =
        new HashMap<String, ExportToFileTestVerifier>();

    private final File m_paths[];
    private final String m_nonce;

    public ExportToFileVerifier(File paths[], String nonce) {
        m_paths = paths;
        m_nonce = nonce;
    }

    public synchronized void addRow(Client client, String tableName, Object partitionHash, Object[] data) throws Exception {
        long partition = ((ClientImpl) client).getPartitionForParameter(VoltType.typeFromObject(partitionHash).getValue(),
                partitionHash);
        ExportToFileTestVerifier verifier = m_verifiers.get(tableName + partition);
        if (verifier == null)
        {
            verifier = new ExportToFileTestVerifier((int) partition);
            m_verifiers.put(tableName + partition, verifier);
        }
        verifier.addRow(data);
    }

    public synchronized void verifyRows() throws Exception {
        TreeMap<Long, List<File>> generations = new TreeMap<Long, List<File>>();
        /*
         * Get all the files for each generation so we process them in the right order
         */
        for (File f : m_paths) {
            for (File f2 : f.listFiles()) {
                if (f2.getName().endsWith("csv")) {
                    Long generation;
                    if (f2.getName().startsWith("active")) {
                        generation = Long.valueOf(f2.getName().split("-")[2]);
                    } else {
                        generation = Long.valueOf(f2.getName().split("-")[1]);
                    }
                    if (!generations.containsKey(generation)) generations.put(generation, new ArrayList<File>());
                    List<File> generationFiles = generations.get(generation);
                    generationFiles.add(f2);

                }
            }
        }

        /*
         * Process the row data in each file
         */
        for (List<File> generationFiles : generations.values()) {
            Collections.sort(generationFiles, new Comparator<File>() {

                @Override
                public int compare(File o1, File o2) {
                    return new Long(o1.lastModified()).compareTo(o2.lastModified());
                }

            });
            for (File f : generationFiles) {
                System.out.println("Processing " + f);
                String tableName;
                if (f.getName().startsWith("active")) {
                    tableName = f.getName().split("-")[3];
                } else {
                    tableName = f.getName().split("-")[2];
                }


                FileInputStream fis = new FileInputStream(f);
                BufferedInputStream bis = new BufferedInputStream(fis);
                InputStreamReader isr = new InputStreamReader(bis, Constants.UTF8ENCODING);
                CSVReader csvreader = new CSVReader(isr);
                String next[] = null;
                while ((next = csvreader.readNext()) != null) {
                    final int partitionId = Integer.valueOf(next[3]);
                    StringBuilder sb = new StringBuilder();
                    for (String s : next) {
                        sb.append(s).append(", ");
                    }
                    System.out.println(sb);
                    ExportToFileTestVerifier verifier = m_verifiers.get(tableName + partitionId);
                    assertThat( next, verifier.isExpectedRow());
                }
            }
        }
    }
}
