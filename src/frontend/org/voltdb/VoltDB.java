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

package org.voltdb;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.ShutdownHooks;
import org.voltdb.config.Configuration;
import org.voltdb.config.VoltDBConfigurer;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.PlatformProperties;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {
    private static AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();


    /** Global constants */
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final int DEFAULT_ZK_PORT = 7181;
    public static final int DEFAULT_IPC_PORT = 10000;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
    public static final int DEFAULT_DR_PORT = 5555;
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;
    public static final int INITIATOR_SITE_ID = 0;
    public static final int SITES_TO_HOST_DIVISOR = 100;
    public static final int MAX_SITES_PER_HOST = 128;

    // Utility to try to figure out if this is a test case.  Various junit targets in
    // build.xml set this environment variable to give us a hint
    public static boolean isThisATest()
    {
        String test = System.getenv().get("VOLT_JUSTATEST");
        if (test == null) {
            test = System.getProperty("VOLT_JUSTATEST");
        }
        if (test != null && test.equalsIgnoreCase("YESYESYES")) {
            return true;
        }
        else {
            return false;
        }
    }

    // The name of the SQLStmt implied by a statement procedure's sql statement.
    public static final String ANON_STMT_NAME = "sql";

    //The GMT time zone you know and love
    public static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT+0");

    //The time zone Volt is actually using, currently always GMT
    public static final TimeZone VOLT_TIMEZONE = GMT_TIMEZONE;

    //Whatever the default timezone was for this locale before we replaced it
    public static final TimeZone REAL_DEFAULT_TIMEZONE;


    private static final RuntimeException DUMMY_EXCEPTION = new RuntimeException();

    // if VoltDB is running in your process, prepare to use UTC (GMT) timezone
    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(GMT_TIMEZONE);
    }

    static {
        REAL_DEFAULT_TIMEZONE = TimeZone.getDefault();
        setDefaultTimezone();
    }

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return !(m_config.m_noLoadLibVOLTDB);
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    /*
     * Create a file that starts with the supplied message that contains
     * human readable stack traces for all java threads in the current process.
     */
    public static void dropStackTrace(String message) {
        if (VoltDB.isThisATest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a stack trace during a junit test.");
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSSZ");
        String dateString = sdf.format(new Date());
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        HostMessenger hm = VoltDB.instance().getHostMessenger();
        int hostId = 0;
        if (hm != null) {
            hostId = hm.getHostId();
        }
        String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
        try {
            PrintWriter writer = new PrintWriter(root + "host" + hostId + "-" + dateString + ".txt");
            writer.println(message);
            printStackTraces(writer);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            try
            {
                VoltLogger log = new VoltLogger("HOST");
                log.error("Error while dropping stack trace for \"" + message + "\"", e);
            }
            catch (RuntimeException rt_ex)
            {
                e.printStackTrace();
            }
        }
    }

    /*
     * Print stack traces for all java threads in the current process to the supplied writer
     */
    public static void printStackTraces(PrintWriter writer) {
        printStackTraces(writer, null);
    }

    /*
     * Print stack traces for all threads in the process to the supplied writer.
     * If a List is supplied then the stack frames for the current thread will be placed in it
     */
    public static void printStackTraces(PrintWriter writer, List<String> currentStacktrace) {
        if (currentStacktrace == null) {
            currentStacktrace = new ArrayList<String>();
        }

        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement ste : myTrace) {
            currentStacktrace.add(ste.toString());
        }

        writer.println();
        writer.println("****** Current Thread ****** ");
        for (String currentStackElem : currentStacktrace) {
            writer.println(currentStackElem);
        }

        writer.println("****** All Threads ******");
        Iterator<Thread> it = traces.keySet().iterator();
        while (it.hasNext())
        {
            Thread key = it.next();
            writer.println();
            StackTraceElement[] st = traces.get(key);
            writer.println("****** " + key + " ******");
            for (StackTraceElement ste : st)
                writer.println(ste);
        }
    }

    public static RuntimeException crashLocalVoltDB(String errMsg) {
        crashLocalVoltDB(errMsg, false, null);
        return DUMMY_EXCEPTION;
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        try {
            OnDemandBinaryLogger.flush();
        } catch (Throwable e) {}

        /*
         * InvocationTargetException suppresses information about the cause, so unwrap until
         * we get to the root cause
         */
        while (thrown instanceof InvocationTargetException) {
            thrown = ((InvocationTargetException)thrown).getCause();
        }

        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        if (VoltDB.isThisATest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a crash file during a junit test.");
        }
        // end test code

        // try/finally block does its best to ensure death, no matter what context this
        // is called in
        try {
            // slightly less important than death, this try/finally block protects code that
            // prints a message to stdout
            try {

                // Even if the logger is null, don't stop.  We want to log the stack trace and
                // any other pertinent information to a .dmp file for crash diagnosis
                List<String> currentStacktrace = new ArrayList<String>();
                currentStacktrace.add("Stack trace from crashLocalVoltDB() method:");

                // Create a special dump file to hold the stack trace
                try
                {
                    TimestampType ts = new TimestampType(new java.util.Date());
                    CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
                    String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
                    PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
                    writer.println("Time: " + ts);
                    writer.println("Message: " + errMsg);

                    writer.println();
                    writer.println("Platform Properties:");
                    PlatformProperties pp = PlatformProperties.getPlatformProperties();
                    String[] lines = pp.toLogLines().split("\n");
                    for (String line : lines) {
                        writer.println(line.trim());
                    }

                    if (thrown != null) {
                        writer.println();
                        writer.println("****** Exception Thread ****** ");
                        thrown.printStackTrace(writer);
                    }

                    printStackTraces(writer, currentStacktrace);
                    writer.close();
                }
                catch (Throwable err)
                {
                    // shouldn't fail, but..
                    err.printStackTrace();
                }

                VoltLogger log = null;
                try
                {
                    log = new VoltLogger("HOST");
                }
                catch (RuntimeException rt_ex)
                { /* ignore */ }

                if (log != null)
                {
                    log.fatal(errMsg);
                    if (thrown != null) {
                        if (stackTrace) {
                            log.fatal("Fatal exception", thrown);
                        } else {
                            log.fatal(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                log.fatal(currentStackElem);
                            }
                        }
                    }
                } else {
                    System.err.println(errMsg);
                    if (thrown != null) {
                        if (stackTrace) {
                            thrown.printStackTrace();
                        } else {
                            System.err.println(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                System.err.println(currentStackElem);
                            }
                        }
                    }
                }
            }
            finally {
                System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
                System.err.println("The log may contain additional information.");
            }
        }
        finally {
            ShutdownHooks.useOnlyCrashHooks();
            System.exit(-1);
        }
    }

    /*
     * For tests that causes failures,
     * allow them stop the crash and inspect.
     */
    public static boolean ignoreCrash = false;

    public static boolean wasCrashCalled = false;

    public static String crashMessage;


    private static VoltDBInterface singleton;

    /**
     * Exit the process with an error message, optionally with a stack trace.
     * Also notify all connected peers that the node is going down.
     */
    public static void crashGlobalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        // end test code

        try {
            // instruct the rest of the cluster to die
            instance().getHostMessenger().sendPoisonPill(errMsg);
            // give the pill a chance to make it through the network buffer
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            // sleep even on exception in case the pill got sent before the exception
            try { Thread.sleep(500); } catch (InterruptedException e2) {}
        }
        // finally block does its best to ensure death, no matter what context this
        // is called in
        finally {
            crashLocalVoltDB(errMsg, stackTrace, t);
        }
    }

    /**
     * Entry point for the VoltDB server process.
     * @param args Requires catalog and deployment file locations.
     */
    public static void main(String[] args) {
        //Thread.setDefaultUncaughtExceptionHandler(new VoltUncaughtExceptionHandler());
        Configuration config = new Configuration(args);
        try {
            if (!config.validate()) {
                System.exit(-1);
            } else {
                
                ctx.register(VoltDBConfigurer.class);
                ctx.getBeanFactory().registerSingleton("configuration", config);
                /*
                BeanDefinition configurationDefinition = BeanDefinitionBuilder //
                        .rootBeanDefinition(Configuration.class) //
                        .addConstructorArgValue(args) //
                        .getBeanDefinition();
                ctx.registerBeanDefinition("configuration", configurationDefinition);
                */
                ctx.refresh();
/*
                initialize(config);
                instance().run();
                */
            }
        }
        catch (OutOfMemoryError e) {
            String errmsg = "VoltDB Main thread: ran out of Java memory. This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, false, e);
        }
    }

    /**
     * Initialize the VoltDB server.
     * @param config  The Configuration to use to initialize the server.
     */
    public static void initialize(Configuration config) {
        m_config = config;
        instance().initialize(config);
    }

    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static VoltDBInterface instance() {
        if(singleton != null) {
            return singleton;
        }
        return ctx.getBean(VoltDBInterface.class);
    }

    /**
     * Useful only for unit testing.
     *
     * Replace the default VoltDB server instance with an instance of
     * VoltDBInterface that is used for testing.
     *
     */
    
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance) {
        singleton = testInstance;
    }
    

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    private static Configuration m_config = new Configuration();
}
