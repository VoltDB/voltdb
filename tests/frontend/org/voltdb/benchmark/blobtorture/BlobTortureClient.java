/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.benchmark.blobtorture;

import java.io.IOException;

import org.voltdb.benchmark.*;

import java.util.logging.Logger;
import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;

/** TPC-C client load generator. */
public class BlobTortureClient extends ClientMain {
    @SuppressWarnings("unused")
    private static final Logger LOG = Logger.getLogger(BlobTortureClient.class.getName());

    private final int m_blobSize;

    public BlobTortureClient(String args[]) {
        super(args);
        int blobSize = 1048576;
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            } else if (parts[0].equals("blobsize")) {
                blobSize = Integer.parseInt(parts[1]);
            }
        }
        m_blobSize = blobSize;
    }

    public static void main(String[] args) {
        ClientMain.main(BlobTortureClient.class, args, false);
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        return new String[] { "RetrieveBlob" };
    }

    private boolean m_didFirstInvocation = false;
    @Override
    protected boolean runOnce() throws IOException {
        if (!m_didFirstInvocation) {
            m_didFirstInvocation = true;
            loadBlobs();
        }
        return selectBlob();
    }

    private long m_partitionCount;
    private final java.util.Random m_random = new java.util.Random();


    @Override
    protected void runLoop() throws IOException {
        loadBlobs();
        while (true) {
            selectBlob();
        }
    }

    private void loadBlobs() {
        try {
            m_partitionCount = m_voltClient.callProcedure("@Statistics", "PARTITIONCOUNT", 0).getResults()[0].asScalarLong();
            System.err.println("Partition count is " + m_partitionCount);
            final StringBuffer sb = new StringBuffer(m_blobSize);
            for (int ii = 0; ii < m_blobSize; ii++) {
                sb.append('b');
            }
            byte blobBytes[] = sb.toString().getBytes("UTF-8");
            for (long ii = 0; ii < m_partitionCount; ii++) {
                System.err.println("Loading blob ID " + ii);
                try {
                    VoltTable vt = m_voltClient.callProcedure("InsertBlob", ii, blobBytes).getResults()[0];
                    vt.advanceRow();
                    final byte bytes[] = vt.getStringAsBytes(1);
                    if (bytes.length != m_blobSize) {
                        System.err.println("Returned blob size was not correct. Expected "
                                + m_blobSize + " but got " + bytes.length);
                        System.exit(-1);
                    }
                } catch (ProcCallException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private final ProcedureCallback m_callback = new ProcedureCallback() {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                System.out.println(clientResponse.getStatusString());
                System.out.println(clientResponse.getException());
                System.exit(-1);
            }
            m_counts[0].incrementAndGet();
            final VoltTable vt = clientResponse.getResults()[0];
            if (!vt.advanceRow()) {
                System.err.println("No rows returned by SelectBlob");
                System.exit(-1);
            }
            final byte bytes[] = vt.getStringAsBytes(1);
            if (bytes.length != m_blobSize) {
                System.err.println("Returned blob size was not correct. Expected "
                        + m_blobSize + " but got " + bytes.length);
                System.exit(-1);
            }
        }

    };

    private boolean selectBlob() throws IOException {
        final long blobId = Math.abs(m_random.nextLong()) % m_partitionCount;
        return m_voltClient.callProcedure( m_callback, "SelectBlob", blobId);
    }

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = BlobTortureProjectBuilder.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends ClientMain> m_loaderClass = null;

    @Override
    protected String getApplicationName() {
        return "Blob Torture";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }
}
