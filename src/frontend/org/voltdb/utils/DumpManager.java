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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import org.voltdb.debugstate.VoltThreadContext;

/**
 * <p>Provide a robust, flexible and totally self-contained way for pieces of the
 * system to dump state (initially to disk).</p>
 *
 * <p>Note that Dumpable classes need to be aware of thread-safeness of their
 * dumps. The system allows the Dumpable implementer to differentiate between
 * unsafe reads and safe reads. It's also just fine to do both.</p>
 *
 */
public class DumpManager extends Thread {

    /**
     * Interface for things that want to respond to
     * systemwide dump requests.
     */
    public static interface Dumpable {

        /**
         * Tell a Dumpable to initiate a dump. This is asking it to call
         * Dumpable.putDump(..) at some point soon, maybe more than once.
         *
         * @param timestamp A timestamp from when the dump was requested that
         * globally identifies the dump.
         */
        public void goDumpYourself(long timestamp);
    }

    /**
     * Since all the goDumpYourself calls might take a big fraction of a second,
     * this class allows them to run in parallel in the background.
     */
    static class DumperThread extends Thread {
        Dumpable m_dumper;
        long m_timestamp;

        DumperThread(Dumpable dumper, long timestamp) {
            m_dumper = dumper;
            m_timestamp = timestamp;
        }

        @Override
        public void run() {
            //System.err.printf("About to dump instance of type: %s\n", m_dumper.getClass().getName());
            m_dumper.goDumpYourself(m_timestamp);
        }
    }

    static final int DUMPMANAGER_PORT = 21217;

    /**
     * Set of active or recently active dumper threads. A list must be kept to keep
     * them from garbage collecting. Dead threads are cleaned up on subsequent calls
     * to dump.
     */
    static HashSet<DumperThread> m_dumperThreads = new HashSet<DumperThread>();

    /**
     * Time of the previous call to requestGlobalDump().
     * Used to eliminate duplicate dumps;
     */
    static long m_previousDump = 0;

    /**
     * Map of registered dumpers by id
     */
    static HashMap<String, WeakReference<Dumpable>> m_dumpers = new HashMap<String, WeakReference<Dumpable>>();

    static DumpManager m_dumpManager = new DumpManager();

    /** Method to start the listener thread for DumpManager */
    public synchronized static void init() {
        if (m_dumpManager.isAlive() == false)
            try {
                m_dumpManager.start();
            }
            catch (Exception e) {
                // nothing to do here
            }
    }

    // prevent this from being instantiated elsewheres
    private DumpManager() {}

    /**
     * Notify the DumpManager that the thing passed in wants to be part of
     * the dump process.
     *
     * @param id The string id of the dumping object (globally unique)
     * @param dumper The object that implements the Dumpable interface that
     * will be involved in all dumps.
     */
    public synchronized static void register(String id, Dumpable dumper) {
        if ((id.indexOf(":") >= 0) || (id.indexOf(":") >= 0))
            throw new RuntimeException("DumpManager Error: Dumper ids cannot contain colons or slashes (" + id + ").");

        // use a weak reference to make cleanup easier
        m_dumpers.put(id, new WeakReference<Dumpable>(dumper));
    }

    /**
     * Ask the DumpManager to get all registered Dumpables to dump state
     * to text files as best as they are able.
     * @param timestamp A timestamp to uniquely identify this dump.
     */
    public synchronized static void requestGlobalDump(long timestamp) {
        // make sure timestamp is unique
        if (timestamp == m_previousDump)
            return;
        m_previousDump = timestamp;

        // actually request all the dumps
        for (WeakReference<Dumpable> dumpRef : m_dumpers.values()) {
            Dumpable dumper = dumpRef.get();
            if (dumper != null) {
                DumperThread dt = new DumperThread(dumper, timestamp);
                dt.start();
                m_dumperThreads.add(dt);
            }
        }

        // remove dead dumpers
        HashSet<DumperThread> deadDumpers = new HashSet<DumperThread>();
        for (DumperThread dt : m_dumperThreads)
            if (dt.isAlive() == false)
                deadDumpers.add(dt);
        for (DumperThread dt : deadDumpers)
            m_dumperThreads.remove(dt);
    }

    /**
     * Respond to a dump request by returning content to the DumpManager.
     *
     * @param id The string id of the dumping object.
     * @param timestamp The timestamp sent with the dump request (unique per dump).
     * @param wasThreadsafeDump Should this dump be considered a unique slice, or was it
     * collected in an unsafe way.
     * @param dumpvalue The contents to be written to the file.
     */
    public synchronized static void putDump(String id, long timestamp, boolean wasThreadsafeDump, VoltThreadContext dumpvalue) {
        File outpath = getPathForDumper(id, timestamp, wasThreadsafeDump);
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(outpath));
            oos.writeObject(dumpvalue);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Given info about the dump, return the path to the file where information should be put.
     *
     * @param id The string id of the dumping object.
     * @param timestamp The timestamp sent with the dump request (unique per dump).
     * @param wasThreadsafeDump Should this dump be considered a unique slice, or was it
     * collected in an unsafe way.
     * @return A File instance representing the place to put content.
     */
    private static File getPathForDumper(String id, long timestamp, boolean wasThreadsafeDump) {
        // make a dateformat that doesn't contain colons or slashes for platform-friendliness
        SimpleDateFormat df = new SimpleDateFormat("yy.MM.dd-HH.mm.ss-SSS'ms'");

        // these will likely be the same thing... but who knows
        String now = df.format(new Date());
        String dumptime = df.format(new Date(timestamp));

        String filename = id + "-" + now + (wasThreadsafeDump ? "-safe" : "-unsafe") + ".voltdump";
        // assume slashes are path separators
        String foldername = "dumps/dump-" + dumptime;

        boolean success = new File(foldername).mkdirs();
        assert(success);

        // assume slashes are path separators
        return new File(foldername + "/" + filename);
    }

    @Override
    public void run() {
        ServerSocket server = null;
        try {
            server = new ServerSocket(DUMPMANAGER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                Socket sock = server.accept();
                DataInputStream dis = new DataInputStream(sock.getInputStream());
                long timestamp = dis.readLong();
                sock.getOutputStream().write('a');
                sock.getOutputStream().flush();
                sock.close();
                requestGlobalDump(timestamp);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Request a dump on a particular hostname.
     * @param hostname
     */
    public static boolean requestDump(String hostname, long timestamp) {
        try {
            // connect to server
            Socket sock = new Socket(hostname, DUMPMANAGER_PORT);

            // write the timestamp to the socket
            DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
            dos.writeLong(timestamp);
            dos.flush();

            // wait for pretty much any response
            int data = sock.getInputStream().read();

            // close the socket and check for sanity
            sock.close();
            return data == 'a';

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Call a dump on localhost by default, but with other parameters optionally
     */
    public static void main(String[] args) {
        String hostnames[] = new String[] { "localhost" };

        // parse an optional arg in kinda a hacky way
        ArrayList<String> hostnameList = new ArrayList<String>();
        if (args.length > 0) {
            for (String arg : args) {
                String hostname = arg.trim();
                if ((hostname.length() == 0) || hostname.startsWith("${"))
                    continue;

                hostnameList.add(hostname);
            }
        }
        if (hostnameList.size() > 0)
            hostnames = hostnameList.toArray(new String[0]);

        long now = System.currentTimeMillis();

        for (String hostname : hostnames) {
            System.out.printf("Trying to connect to %s\n", hostname);
            System.out.flush();

            if (requestDump(hostname, now))
                System.out.printf("Asyncronous dump in progress.\n");
            else
                System.out.printf("Dump Failed. See stack trace.\n");
            System.out.flush();
        }
    }
}
