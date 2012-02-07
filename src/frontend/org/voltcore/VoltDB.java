/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltcore;

import java.util.Map;
import org.voltcore.logging.VoltLogger;

/**
 * <code>VoltDB</code> is the main class for VoltDB server.
 * It sets up global objects and then starts the individual threads
 * for the <code>ThreadManager</code>s.
 */
public class VoltDB {

    /** Global constants */
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
    public static final int DEFAULT_DR_PORT = 5555;



    static final int INITIATOR_SITE_ID = 0;
    public static final int DTXN_MAILBOX_ID = 0;
    public static final int AGREEMENT_MAILBOX_ID = 1;
    public static final int STATS_MAILBOX_ID = 2;

    // temporary for single partition testing
    static final int FIRST_SITE_ID = 1;

    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;

    // Persistent nodes (mostly directories) that need to be created on startup
    public static final String[] ZK_HIERARCHY =
        {"/hosts",
         "/hostids",
         "/ready_hosts"};

    /**
     * Wrapper for crashLocalVoltDB() to keep compatibility with >100 calls.
     */
    @Deprecated
    public static void crashVoltDB() {
        crashLocalVoltDB("Unexpected crash", true, null);
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     *
     * In the future it would be nice to notify any non-failed subsystems
     * that the node is going down. For now, just die.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
//        if (instance().ignoreCrash()) {
//            return;
//        }

        VoltLogger log = new VoltLogger("HOST");

        if (t != null) {
            //log.fatal(errMsg, t);
            System.out.print(errMsg);
            t.printStackTrace();
        } else {
            System.out.println(errMsg);
            //log.fatal(errMsg);
        }

        if (stackTrace) {
            StringBuilder sb = new StringBuilder("Stack trace from crashVoltDB() method:\n");

            Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
            StackTraceElement[] myTrace = traces.get(Thread.currentThread());
            for (StackTraceElement ste : myTrace) {
                sb.append(ste.toString()).append("\n");
            }
            System.out.println(sb);
            //log.fatal(sb);
        }

        System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
        System.err.println("The log may contain additional information.");
        System.exit(-1);
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     * Also notify all connected peers that the node is going down.
     *
     * In the future it would be nice to notify any non-failed subsystems
     * that the node is going down. For now, just die.
     */
    public static void crashGlobalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
//        if (instance().ignoreCrash()) {
//            return;
//        }
        try {
//            ((HostMessenger) instance().getMessenger()).sendPoisonPill(errMsg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try { Thread.sleep(500); } catch (InterruptedException e) {}
        crashLocalVoltDB(errMsg, stackTrace, t);
    }

}
