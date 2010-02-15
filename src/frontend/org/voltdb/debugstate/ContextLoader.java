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

package org.voltdb.debugstate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;

import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState.WorkUnitState;
import org.voltdb.debugstate.MailboxHistory.MessageState;
import org.voltdb.dtxn.InFlightTxnState;

public class ContextLoader {

    public static class SystemState {
        public InitiatorContext[] initiators;
        public ExecutorContext[] executors;
        public PlannerThreadContext[] planners;
    }

    static void dumpAllMessagesAboutBlockedTxn(SystemState state, long txnId) {
        System.out.println("INITITATORS:");
        for (InitiatorContext ic : state.initiators) {
            System.out.println("STATE:");
            for (InFlightTxnState ift : ic.inFlightTxns) {
                if (ift.txnId == txnId) {
                    System.out.println(ift.toString());
                }
            }
            System.out.println("MESSAGES RECEIVED:");
            for (MessageState message : ic.mailboxHistory.messagesReceived) {
                if (message.txnId == txnId)
                    System.out.println(message.summary);
            }
            System.out.println("MESSAGES SENT:");
            for (MessageState message : ic.mailboxHistory.messagesSent) {
                if (message.txnId == txnId)
                    System.out.println(message.summary);
            }
        }
        System.out.println();

        System.out.println("EXECUTORS:");
        for (ExecutorContext ec : state.executors) {
            System.out.println("STATE:");
            if (ec.activeTransaction != null) {
                if (ec.activeTransaction.txnId == txnId) {
                    System.out.println(ec.activeTransaction.toString());
                }
            }
            for (ExecutorTxnState ets : ec.queuedTransactions) {
                if (ets.txnId == txnId) {
                    System.out.println(ets.toString());
                }
            }
            System.out.println("MESSAGES RECEIVED:");
            for (MessageState message : ec.mailboxHistory.messagesReceived) {
                if (message.txnId == txnId)
                    System.out.println(message.summary);
            }
            System.out.println("MESSAGES SENT:");
            for (MessageState message : ec.mailboxHistory.messagesSent) {
                if (message.txnId == txnId)
                    System.out.println(message.summary);
            }
        }
        System.out.println();
    }

    static long findBlockedTxnId(SystemState state) {
        long retval = 0;
        long activeTxnId = 0;

        for (ExecutorContext ec : state.executors) {
            ExecutorTxnState ets = ec.activeTransaction;
            if (ets != null) {
                assert((activeTxnId == 0) || (activeTxnId == ets.txnId));

                if (ets.workUnits != null) {
                    for (WorkUnitState wus : ets.workUnits) {
                        if (wus.outstandingDependencyCount > 0) {
                            assert(retval == 0);
                            retval = ets.txnId;
                        }
                    }
                }
            }
        }

        return retval;
    }


    /**
     * Present a user with a list of options and let them choose one.
     * @param dumps Valid dumps to choose from.
     * @return The chosen dump.
     */
    static File chooseDump(ArrayList<File> dumps) {
        File retval = null;
        InputStreamReader inp = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(inp);

        while (retval == null) {
            System.out.println("Please choose a dump:\n");
            for (int i = 0; i < dumps.size(); i++)
                System.out.printf(" %d) %s\n", i + 1, dumps.get(i).getName());
            System.out.print("> ");
            String choice = null;
            try {
                choice = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Unable to choose a dump.");
                System.exit(-1);
            }

            int numChoice = Integer.parseInt(choice.trim());
            if ((numChoice > 0) && (numChoice <= dumps.size()))
                retval = dumps.get(numChoice - 1);
            else
                System.out.println("Choice not recognized. Please try again.\n");
        }


        return retval;
    }

    static class LoaderThread extends Thread {
        @Override
        public void run() {
            File inFile = null;

            while (true) {
                synchronized(inFiles) {
                    inFile = inFiles.pollFirst();
                }
                if (inFile == null)
                    return;

                try {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inFile));
                    Object temp = ois.readObject();
                    if (temp instanceof VoltThreadContext) {
                        synchronized(contexts) {
                            contexts.add((VoltThreadContext) temp);
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static ArrayDeque<File> inFiles = new ArrayDeque<File>();
    static ArrayList<VoltThreadContext> contexts = new ArrayList<VoltThreadContext>();

    /**
     * Once a dump is chosen, load the object-graphs it contains into memory
     * @param dump The chosen dump directory
     */
    static VoltThreadContext[] loadDump(File dump) {
        assert(dump.isDirectory());

        // find all matching files
        int foundfiles = 0;
        for (File contextFile : dump.listFiles()) {
            if (contextFile.getName().endsWith(".voltdump") == false) continue;
            foundfiles++;
            inFiles.add(contextFile);
        }

        // create a threadpool
        LoaderThread[] loaderpool = new LoaderThread[8];
        for (int i = 0; i < 8; i++) {
            loaderpool[i] = new LoaderThread();
            loaderpool[i].start();
        }

        // join on the threadpool
        for (LoaderThread loader : loaderpool)
            try {
                loader.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        // check that we got something
        if (contexts.size() == 0) {
            System.err.println("No loadable dumpcontexts found. Exiting.");
            System.exit(-1);
        }

        System.out.printf("Loaded %d/%d dumps inside dump folder.\n", contexts.size(), foundfiles);

        VoltThreadContext[] retval = new VoltThreadContext[contexts.size()];
        for (int i = 0; i < contexts.size(); i++)
            retval[i] = contexts.get(i);

        return retval;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please pass the path to dump(s) as a command line argument.");
            return;
        }

        File dumpsDir = new File(args[0]);
        if (dumpsDir.isDirectory() == false) {
            System.out.println("Path does not point to a valid directory");
            return;
        }

        File[] dumps = dumpsDir.listFiles();
        ArrayList<File> validDumps = new ArrayList<File>();
        boolean isDump = true;

        // if the dir contains no subdirs, it's probably a dump
        for (File dump : dumps)
            if (dump.isDirectory()) isDump = false;

        if (isDump) {
            validDumps.add(dumpsDir);
        }
        else {
            for (File dump : dumps) {
                if (dump.isDirectory() == false) continue;
                validDumps.add(dump);
            }
        }

        File dump = null;
        if (validDumps.size() == 0) {
            System.out.println("No valid dumps found. Possible empty directory error.");
            return;
        }
        else if (validDumps.size() > 1) {
            dump = chooseDump(validDumps);
        }
        else {
            dump = validDumps.get(0);
        }

        assert(dump != null);

        VoltThreadContext[] contexts = loadDump(dump);

        SystemState state = new SystemState();
        ArrayList<PlannerThreadContext> ptcs = new ArrayList<PlannerThreadContext>();
        ArrayList<InitiatorContext> ics = new ArrayList<InitiatorContext>();
        ArrayList<ExecutorContext> ecs = new ArrayList<ExecutorContext>();

        for (VoltThreadContext c : contexts) {
            if (c instanceof PlannerThreadContext)
                ptcs.add((PlannerThreadContext) c);
            else if (c instanceof InitiatorContext)
                ics.add((InitiatorContext) c);
            else if (c instanceof ExecutorContext)
                ecs.add((ExecutorContext) c);
            else
                assert(false);
        }
        state.planners = ptcs.toArray(new PlannerThreadContext[0]);
        Arrays.sort(state.planners);
        state.initiators = ics.toArray(new InitiatorContext[0]);
        Arrays.sort(state.initiators);
        state.executors = ecs.toArray(new ExecutorContext[0]);
        Arrays.sort(state.executors);

        long blockedTxnId = findBlockedTxnId(state);

        dumpAllMessagesAboutBlockedTxn(state, blockedTxnId);
    }

}
