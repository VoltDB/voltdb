/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;

/**
 * A hacky singleton which provides simple coordinated registration of ShutdownHooks within Volt.
 * If you want to add a shutdown hook via Runtime.getRuntime.addShutdownHook, you should probably
 * add it here instead.
 */
public class ShutdownHooks
{
    /**
     * Provide some initial constants for shutdown order
     */
    public static int FIRST = 1;
    public static int MIDDLE = 50;
    // Async volt logger needs to purge before log4j shuts down
    public static int VOLT_LOGGER = 99;
    // Kill log4j
    public static int VOLT_LOG4J = 100;

    class ShutdownTask {
        final int m_priority;
        final boolean m_runOnCrash;
        final Runnable m_action;

        ShutdownTask(int priority, boolean runOnCrash, Runnable action)
        {
            m_priority = priority;
            m_runOnCrash = runOnCrash;
            m_action = action;
        }
    }

    private static VoltLogger consoleLog = new VoltLogger("CONSOLE");

    private static ShutdownHooks m_instance = null;

    static {
        m_instance = new ShutdownHooks();
    }

    /**
     * Register an action to be run when the JVM exits.
     * @param priority The priority level at which this action should be run.  Lower values will run earlier.
     * @param runOnCrash  Whether or not this action should be performed if the server is shutting down
     *                    due to a call to crashVoltDB()
     * @param action   A Runnable containing the action to be run on shutdown.
     */
    public static void registerShutdownHook(int priority, boolean runOnCrash, Runnable action)
    {
        m_instance.addHook(priority, runOnCrash, action);
    }

    /**
     * Unfortunately, lots of stuff pulls in VoltLogger, which pulls in this shutdown hook,
     * which then prints scary warnings about shutdowns.  Flail arms around and indicate to the shutdown
     * hooks that they're actually being called from within a VoltDB server.  To avoid
     * having every command line utility have to instruct the shutdown hooks, we'll default to
     * not being a server and then have RealVoltDB do the right thing, Spike-Lee-style
     */
    public static void enableServerStopLogging()
    {
        m_instance.youAreNowAServer();
    }

    /**
     * Indicate that only actions that should run on crashVoltDB() should be run.
     */
    public static void useOnlyCrashHooks()
    {
        m_instance.crashing();
    }

    private Thread m_globalHook = new Thread() {
        @Override
        public void run() {
            runHooks();
        }
    };

    private SortedMap<Integer, List<ShutdownTask>> m_shutdownTasks;
    private boolean m_crashing = false;
    private boolean m_iAmAServer = false;

    private ShutdownHooks() {
        m_shutdownTasks = new TreeMap<Integer, List<ShutdownTask>>();
        Runtime.getRuntime().addShutdownHook(m_globalHook);
    }

    private synchronized void addHook(int priority, boolean runOnCrash, Runnable action)
    {
        List<ShutdownTask> tasks = m_shutdownTasks.get(priority);
        if (tasks == null) {
            tasks = new ArrayList<ShutdownTask>();
            m_shutdownTasks.put(priority, tasks);
        }
        tasks.add(new ShutdownTask(priority, runOnCrash, action));
    }

    private synchronized void runHooks()
    {
        if (m_iAmAServer && !m_crashing) {
            consoleLog.warn("The VoltDB server will shut down due to a control-C or other JVM exit.");
        }
        for (Entry<Integer, List<ShutdownTask>> tasks : m_shutdownTasks.entrySet()) {
            for (ShutdownTask task : tasks.getValue()) {
                if (!m_crashing || (m_crashing && task.m_runOnCrash)) {
                    try {
                        task.m_action.run();
                    } catch (Exception e) {
                        consoleLog.warn("Exception while running shutdown hooks.", e);
                    }
                }
            }
        }
    }

    private synchronized void crashing()
    {
        m_crashing = true;
    }

    private synchronized void youAreNowAServer()
    {
        m_iAmAServer = true;
    }
};
