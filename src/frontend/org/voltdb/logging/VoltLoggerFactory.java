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

package org.voltdb.logging;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.Loader;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.TTCCLayout;
import org.apache.log4j.Level;
import org.voltdb.VoltDB;

/**
 * A factory for log4j loggers that attempts to set the resource bundle of the root logger when the class is loaded
 */
public final class VoltLoggerFactory implements LoggerFactory {

    static volatile AsyncAppender globalDefaultAppender;
    static {
        ResourceBundle rb = null;
        try {
            rb = ResourceBundle.getBundle("org/voltdb/utils/voltdb_logstrings");
        } catch (MissingResourceException e) {
            System.err.println("Couldn't find voltdb_logstrings resource bundle. Should be in voldb_logstrings.properties.");
            e.printStackTrace(System.err);
            VoltDB.crashVoltDB();
        }
        Logger.getRootLogger().setResourceBundle(rb);

        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    @Override
                    public void run() {
                        LogManager.shutdown();
                    }
                });

        String log4JConfigFile = System.getProperty("log4j.configuration");

        if (log4JConfigFile != null) {
            URL url = null;
            try {
                url = new URL(log4JConfigFile);
            } catch (MalformedURLException ex) {
                url = Loader.getResource(log4JConfigFile);
            }
            if (url == null) {
                System.err.println("Couldn't find logging configuration " + log4JConfigFile);
                log4JConfigFile = null;
            }
        }

        if (log4JConfigFile == null) {
            /*
             * If you change this string it will break the adhoc planner.
             * The adhoc planner checks for this string and skips it when reading
             * output from the adhoc planner. If the match fails when the string changes
             * it is interpereted as an error
             */
            System.err.println("No logging configuration supplied via -Dlog4j.configuration. " +
                    "Supplying default config that logs INFO or higher to STDOUT");
            TTCCLayout layout = new TTCCLayout();
            AsyncAppender asyncAppender = new AsyncAppender();
            asyncAppender.setName("Default global appender");
            globalDefaultAppender = asyncAppender;
            asyncAppender.setBlocking(true);
            ConsoleAppender consoleAppender = new ConsoleAppender(layout, ConsoleAppender.SYSTEM_OUT);
            asyncAppender.addAppender(consoleAppender);
            consoleAppender.activateOptions();
            asyncAppender.activateOptions();
            Logger.getRootLogger().removeAllAppenders();
            Logger.getRootLogger().addAppender(asyncAppender);
            Logger.getRootLogger().setLevel(Level.INFO);
        } else {
            globalDefaultAppender = null;
        }
    }

    private VoltLoggerFactory() {

    }

    private final static VoltLoggerFactory m_instance = new VoltLoggerFactory();

    public static LoggerFactory instance() {
        return m_instance;
    }

    @Override
    public Logger makeNewLoggerInstance(String name) {
        /**
         * The global default appender should only effect
         * the user visible loggers and not the private org.volt loggers
         */
        if (globalDefaultAppender == null) {
            return Logger.getLogger(name);
        } else {
            Logger l = Logger.getLogger(name);
            if(name.startsWith("org.voltdb")) {
                l.setLevel(Level.OFF);
            }
            return l;
        }
    }

}
