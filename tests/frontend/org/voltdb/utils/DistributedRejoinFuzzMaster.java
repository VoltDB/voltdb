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
package org.voltdb.utils;

import java.util.ArrayList;

import org.voltdb.benchmark.KillStragglers;
import org.voltdb.processtools.ProcessData;
import org.voltdb.processtools.ProcessSetManager;
import org.voltdb.processtools.SSHTools;

public class DistributedRejoinFuzzMaster {

    private static final String nodes[] = new String[] { "volt7b", "volt7c", "volt7d", "volt7e" };

    private static final String safeNode = "volt7b";

    private static final String rejoinCommand = "java -classpath /home/izzy/joiner/voltdb-1.2.1/obj/release/dist/voltdb/voltdb-1.2.1.06.jar -Djava.library.path=/home/izzy/joiner/voltdb-1.2.1/obj/release/dist/voltdb -Xmx2048m -XX:-ReduceInitialCardMarks -XX:HeapDumpPath=/tmp -XX:+HeapDumpOnOutOfMemoryError -Dlog4j.configuration=file:log-server.xml org.voltdb.VoltDB catalog catalog.jar deployment deployment.xml rejoinhost ";

    private static final String startCommand = "java -classpath /home/izzy/joiner/voltdb-1.2.1/obj/release/dist/voltdb/voltdb-1.2.1.06.jar -Djava.library.path=/home/izzy/joiner/voltdb-1.2.1/obj/release/dist/voltdb -Xmx2048m -XX:-ReduceInitialCardMarks -XX:HeapDumpPath=/tmp -XX:+HeapDumpOnOutOfMemoryError -Dlog4j.configuration=file:log-server.xml org.voltdb.VoltDB catalog catalog.jar deployment deployment.xml";

    private static final String remotePath = "/home/izzy/joiner/voltdb-1.2.1/tests/test_apps/deletes";

    private static final String remoteUser = "izzy";
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        for (String node : nodes) {
            new KillStragglers(remoteUser,
                                      node,
                                      remotePath).run();
        }
        for (String node : nodes) {
            Runtime.getRuntime().addShutdownHook(
                    new KillStragglers(remoteUser,
                                       node,
                                       remotePath));
        }

        SSHTools ssh = new SSHTools(remoteUser);

        ArrayList<String[]> startCommands = new ArrayList<String[]>();
        for (String node : nodes) {
            startCommands.add( ssh.convert(node, remotePath, startCommand));
        }

        ProcessSetManager psm = new ProcessSetManager();
        int ii = 0;
        for (String startCommand[] : startCommands) {
            psm.startProcess(nodes[ii++], startCommand);
        }

        // WAIT FOR SERVERS TO BE READY
        String readyMsg = "Server completed initialization.";
        ProcessData.OutputLine line = psm.nextBlocking();
        while(line.message.equals(readyMsg) == false) {
            System.err.printf("(%s): \"%s\"\n", line.processName, line.message);
            line = psm.nextBlocking();
        }

        // if the rejoin times out, the node's name will be recorded here
        String retryRejoinNode = null;

        java.util.Random r = new java.util.Random(0);
        while (true) {
            Thread.sleep(15000);
            String forWhomTheBellTolls = retryRejoinNode;
            if (forWhomTheBellTolls == null)
            {
                forWhomTheBellTolls = nodes[r.nextInt(nodes.length)];
                while (forWhomTheBellTolls.equals(safeNode)) {
                    forWhomTheBellTolls = nodes[r.nextInt(nodes.length)];
                }


                psm.killProcess(forWhomTheBellTolls);
                new KillStragglers(remoteUser,
                                   forWhomTheBellTolls,
                                   remotePath).run();
                System.out.printf("****************** killing process %s\n",forWhomTheBellTolls);

                String failureDetectionComplete = new String("Handling node faults");
                int detectionCompleteCount = 0;
                while (detectionCompleteCount != ((nodes.length - 1) * 2)) {
                    line = psm.nextBlocking();
                    if (line.message.contains(failureDetectionComplete)) {
                        detectionCompleteCount++;
                        System.out.printf("****************** detectionCompleteCount %d\n",detectionCompleteCount);
                    }
                    System.err.printf("(%s): \"%s\"\n", line.processName, line.message);
                }
                Thread.sleep(1000);
            }

            String recoverConnectTo = safeNode;
            //String recoverConnectTo = null;
            //while (recoverConnectTo == null) {
            //    int index = r.nextInt(nodes.length);
            //    if (nodes[index] != forWhomTheBellTolls) {
            //        recoverConnectTo = nodes[index];
            //    }
            //}

            String recoverCommand[] = ssh.convert(forWhomTheBellTolls, remotePath, rejoinCommand + recoverConnectTo);
            psm.startProcess( forWhomTheBellTolls, recoverCommand);

            String recoverMessage = "Node recovery completed after";
            String retryMessage = "Timed out waiting for other nodes to connect";
            line = psm.nextBlocking();
            System.err.printf("(%s): \"%s\"\n", line.processName, line.message);
            boolean recovered = line.message.contains(recoverMessage);
            boolean retry = line.message.contains(retryMessage);
            while(!recovered && !retry)
            {
                System.err.printf("(%s): \"%s\"\n", line.processName, line.message);
                line = psm.nextBlocking();
                recovered = line.message.contains(recoverMessage);
                retry = line.message.contains(retryMessage);
            }
            if (!recovered)
            {
                assert(retry);
                retryRejoinNode = forWhomTheBellTolls;
            }
            else
            {
                retryRejoinNode = null;
            }
        }
    }

}
