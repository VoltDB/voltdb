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

package org.voltdb.benchmark.multisite;

import java.io.IOException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.multisite.procedures.ChangeSeat;
import org.voltdb.benchmark.multisite.procedures.FindOpenSeats;
import org.voltdb.benchmark.multisite.procedures.UpdateReservation;

public class MultisiteClient extends ClientMain {

    /*
     * BenchmarkController and ClientMain requirements
     */

    public static enum Transaction {
        kFindOpenSeats("Find open seats"),
        kUpdateReservation("Update reservation"),
        kChangeSeat("Change seat");
        private Transaction(String displayName) { this.displayName = displayName; }
        public final String displayName;
    }

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(MultisiteClient.class, args, false);
    }

    public MultisiteClient(String[] args)
    {
        super(args);
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1)
                continue;

            if (parts[1].startsWith("${"))
                continue;

            if (parts[0].equals("sf"))
                m_scalefactor = Integer.parseInt(parts[1]);
            else if (parts[0].equals("multipartition"))
                m_multipartition = Integer.parseInt(parts[1]);
        }
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.displayName;
        }
        return names;
    }

    @Override
    protected void runLoop() {
        try {
            while (true) {
                m_voltClient.backpressureBarrier();
                executeTransaction();
            }
        } catch (IOException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends VoltProjectBuilder>
        m_projectBuilderClass = MultisiteProjectBuilder.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends ClientMain>
        m_loaderClass = org.voltdb.benchmark.multisite.Loader.class;


    /*
     * Application logic.
     */

    private int m_multipartition = 10;
    private int m_scalefactor = 1;

    // used only for the random number helpers in this context
    private final Loader m_loader = new Loader(new String[] {});

    private boolean executeTransaction() throws IOException {
        int val = m_loader.number(1,100);
        boolean queued = false;
        if (val <= m_multipartition) {
            queued = runUpdateReservation();
        }

        if ((val % 2) == 1) {
            queued = runFindOpenSeats();
        }

        queued = runChangeSeat();
        return queued;
    }

    class RunChangeSeatCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            m_counts[Transaction.kChangeSeat.ordinal()].incrementAndGet();
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                assert(clientResponse.getResults().length == 1);
                assert(clientResponse.getResults()[0].getRowCount() == 1);
                assert(clientResponse.getResults()[0].asScalarLong() == 1 ||
                       clientResponse.getResults()[0].asScalarLong() == 0);
            }
        }
    }

    private boolean runChangeSeat() throws IOException {
        // fid, cid, seatnum

        // cids are assigned to flights such that:
        // cid = (maxfid * (seat -1) + fid) % maxcust.
        // see the Loader reservation loader for details

        int maxfid = Loader.kMaxFlights / m_scalefactor;
        int maxcid = Loader.kMaxCustomers / m_scalefactor;
        int fid = m_loader.number(1, maxfid);
        int seat = m_loader.number(1, 150);
        int cid = (maxfid * (seat -1 ) + fid) % maxcid;
        if (cid == 0) cid = maxcid; // cids are 1-based

        return m_voltClient.callProcedure(new RunChangeSeatCallback(),
                                   ChangeSeat.class.getSimpleName(),
                                   fid, cid, seat);
    }

    class RunUpdateReservationCallback implements ProcedureCallback {
        private final int m_rid, m_maxfid;

        RunUpdateReservationCallback(int rid, int maxfid) {
            m_rid = rid;
            m_maxfid = maxfid;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            m_counts[Transaction.kUpdateReservation.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            if (m_rid < (150 * .5 * m_maxfid)) {
                assert (results.length == 1);
                assert (results[0].getRowCount() == 1);
                assert (results[0].asScalarLong() == 1);
            }
        }

    }

    private boolean runUpdateReservation() throws IOException {
        // there are roughly 150 * 0.80 * maxflights reservations in the system
        int maxfid = Loader.kMaxFlights / m_scalefactor;
        int rid = m_loader.number(1, (int) (150 * 0.8 * maxfid));
        int value = m_loader.number(1, 1 << 20);

        return m_voltClient.callProcedure(new RunUpdateReservationCallback(rid, maxfid),
                                   UpdateReservation.class.getSimpleName(),
                                   rid, value);
    }

    class RunFindOpenSeats implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            m_counts[Transaction.kFindOpenSeats.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() < 150);
            // there is some tiny probability of an empty flight .. maybe 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            assert (results[0].getRowCount() > 1);
        }

    }

    private boolean runFindOpenSeats() throws IOException {
        // find a random fid
        int maxfid = Loader.kMaxFlights / m_scalefactor;
        int fid = m_loader.number(1, maxfid);

        return m_voltClient.callProcedure(new RunFindOpenSeats(),
                                   FindOpenSeats.class.getSimpleName(),
                                   fid);
    }

    @Override
    protected String getApplicationName() {
        return "Multisite Benchmark";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }
}
