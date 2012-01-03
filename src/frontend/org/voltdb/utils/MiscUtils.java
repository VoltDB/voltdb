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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.logging.VoltLogger;

public class MiscUtils {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     * Simple code to copy a file from one place to another...
     * Java should have this built in... stupid java...
     */
    public static void copyFile(String fromPath, String toPath) throws Exception {
        File inputFile = new File(fromPath);
        File outputFile = new File(toPath);

        FileReader in = new FileReader(inputFile);
        FileWriter out = new FileWriter(outputFile);
        int c;

        while ((c = in.read()) != -1)
          out.write(c);

        in.close();
        out.close();
    }

    public static final int[] toArray(Set<Integer> set) {
        int retval[] = new int[set.size()];
        int ii = 0;
        for (Integer i : set) {
            retval[ii++] = i;
        }
        return retval;
    }

    /**
     * Try to load a PRO class. If it's running the community edition, an error
     * message will be logged and null will be returned.
     *
     * @param classname The class name of the PRO class
     * @param feature The name of the feature
     * @param suppress true to suppress the log message
     * @return null if running the community edition
     */
    public static Class<?> loadProClass(String classname, String feature, boolean suppress) {
        try {
            Class<?> klass = Class.forName(classname);
            return klass;
        } catch (ClassNotFoundException e) {
            if (!suppress) {
                hostLog.warn("Cannot load " + classname + " in VoltDB community edition. " +
                             feature + " will be disabled.");
            }
            return null;
        }
    }

    /**
     * Validate the signature and business logic enforcement for a license.
     * @param pathToLicense
     * @param numberOfNodes
     * @return true if the licensing constraints are met
     */
    public static boolean validateLicense(String pathToLicense, int numberOfNodes) {
        // verify the file exists.
        File licenseFile = new File(pathToLicense);
        if (licenseFile.exists() == false) {
            hostLog.fatal("Unable to open license file: " + pathToLicense);
            return false;
        }

        // boilerplate to create a license api interface
        LicenseApi licenseApi = null;
        Class<?> licApiKlass = MiscUtils.loadProClass("org.voltdb.licensetool.LicenseApiImpl",
                                                      "License API", false);

        if (licApiKlass != null) {
            try {
                licenseApi = (LicenseApi)licApiKlass.newInstance();
            } catch (InstantiationException e) {
                hostLog.fatal("Unable to process license file: could not create license API.");
                return false;
            } catch (IllegalAccessException e) {
                hostLog.fatal("Unable to process license file: could not create license API.");
                return false;
            }
        }

        if (licenseApi == null) {
            hostLog.fatal("Unable to load license file: could not create license API.");
            return false;
        }

        // Initialize the API. This parses the file but does NOT verify signatures.
        if (licenseApi.initializeFromFile(licenseFile) == false) {
            hostLog.fatal("Unable to load license file: could not parse license.");
            return false;
        }

        // Perform signature verification - detect modified files
        if (licenseApi.verify() == false) {
            hostLog.fatal("Unable to load license file: could not verify license signature.");
            return false;
        }

        Calendar now = GregorianCalendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy");
        String expiresStr = sdf.format(licenseApi.expires().getTime());
        boolean valid = true;

        if (now.after(licenseApi.expires())) {
            if (licenseApi.isTrial()) {
                hostLog.fatal("VoltDB trial license expired on " + expiresStr + ".");
                return false;
            }
            else {
                // Expired commercial licenses are allowed but generate log messages.
                hostLog.error("Warning, VoltDB commercial license expired on " + expiresStr + ".");
                valid = false;
            }
        }

        // print out trial success message
        if (licenseApi.isTrial()) {
            hostLog.info("Starting VoltDB with trial license. License expires on " + expiresStr + ".");
            return true;
        }

        // ASSUME CUSTOMER LICENSE HERE

        // single node product strictly enforces the single node detail...
        if (licenseApi.maxHostcount() == 1 && numberOfNodes > 1) {
            hostLog.fatal("Warning, VoltDB commercial license for a 1 node " +
                    "attempted for use with a " + numberOfNodes + " node cluster." +
                    " A single node subscription is only valid with a single node cluster.");
            return false;
        }
        // multi-node commercial licenses only warn
        else if (numberOfNodes > licenseApi.maxHostcount()) {
            hostLog.error("Warning, VoltDB commercial license for " + licenseApi.maxHostcount() +
                          " nodes, starting cluster with " + numberOfNodes + " nodes.");
            valid = false;
        }

        // this gets printed even if there are non-fatal problems, so it
        // injects the word "invalid" to make it clear this is the case
        String msg = String.format("Starting VoltDB with %scommercial license. " +
                                   "License for %d nodes expires on %s.",
                                   (valid ? "" : "invalid "),
                                   licenseApi.maxHostcount(),
                                   expiresStr);
        hostLog.info(msg);

        return true;
    }

    /**
     * Check that RevisionStrings are properly formatted.
     * @param fullBuildString
     * @return build revision # (SVN), build hash (git) or null
     */
    public static String parseRevisionString(String fullBuildString) {
        String build = "";

        // Test for SVN revision string - example: https://svn.voltdb.com/eng/trunk?revision=2352
        String[] splitted = fullBuildString.split("=", 2);
        if (splitted.length == 2) {
            build = splitted[1].trim();
                if (build.length() == 0) {
                        return null;
                        }
                return build;

                }

        // Test for git build string - example: 2.0 voltdb-2.0-70-gb39f43e-dirty
        Pattern p = Pattern.compile("-(\\d*-\\w{8}(?:-.*)?)");
        Matcher m = p.matcher(fullBuildString);
        if (! m.find())
                return null;
        build = m.group(1).trim();
        if (build.length() == 0) {
            return null;
        }
        return build;

    }

    /*
     * Have shutdown actually means shutdown. Tasks that need to complete should use
     * futures.
     */
    public static ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor(String name, int poolSize, int stackSize) {
        ScheduledThreadPoolExecutor ses = new ScheduledThreadPoolExecutor(poolSize, getThreadFactory(name));
        ses.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        ses.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return ses;
    }

    public static ThreadFactory getThreadFactory(String name) {
        return getThreadFactory(name, 1024 * 1024);
    }

    public static ThreadFactory getThreadFactory(final String name, final int stackSize) {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(null, r, name, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }

    private static String checkForJavaHomeInEnvironment() throws Exception {
        String javahome = System.getenv("JAVA_HOME");
        if (javahome != null) {
            File f = new File(new File(javahome, "bin"), "java");
            if (f.exists() && f.canExecute()) {
                return f.getAbsolutePath();
            } else {
                hostLog.warn("JAVA_HOME environment variable (" + javahome +
                        ") was set, but could not find an executable java binary at " + f.getAbsolutePath());
            }
        } else {
            hostLog.warn("JAVA_HOME environment variable was not set, couldn't be used to find a java binary");
        }
        return null;
    }

    private static String checkForJavaInPath() throws Exception {
        Process p = Runtime.getRuntime().exec("which java");
        p.waitFor();
        InputStream is = p.getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int read = 0;
        while ((read = is.read()) != -1) {
            baos.write(read);
        }
        String java = new String(baos.toByteArray(), "UTF-8").trim();
        if (java != null) {
            File f = new File(java);
            if (f.exists() && f.canExecute()) {
                return f.getAbsolutePath();
            } else {
                hostLog.warn("Attempted to discover java binary from PATH using 'which java' (" +
                        java + ") but an executable binary was not found at " + f.getAbsolutePath());
            }
        }
        return null;
    }

    public static String getJavaPath() throws Exception {
        String javahome = System.getProperties().getProperty("java.home");
        if (javahome != null) {
            File f = new File(javahome + "/bin/java");
            if (f.exists() && f.canExecute()) {
                return f.getAbsolutePath();
            } else {
                hostLog.warn("Couldn't find java binary in java home (" + javahome + ") defined by " +
                        "the java.home system property. Path checked was " + f.getAbsolutePath());
            }
        } else {
            hostLog.warn("java.home system property not defined");
        }
        String javaPath = checkForJavaHomeInEnvironment();
        if (javaPath == null) {
          javaPath = checkForJavaInPath();
        }
        if (javaPath == null) {
            hostLog.error(
                    "Could not find executable java binary in the java.home property specified by " +
                    "the JVM, or in the Java home specified  by the JAVA_HOME environment variable, " +
                    " or in the PATH");
            throw new Exception("Could not find java binary");
        }
        return javaPath;
    }

    public static String formatHostMetadataFromJSON(String json) {
        try {
            JSONObject obj = new JSONObject(json);
            StringBuilder sb = new StringBuilder();

            JSONArray interfaces = (JSONArray) obj.get("interfaces");

            for (int ii = 0; ii < interfaces.length(); ii++) {
                sb.append(interfaces.getString(ii));
                if (ii + 1 < interfaces.length()) {
                    sb.append(" ");
                }
            }
            sb.append(" ");
            sb.append(obj.getString("clientPort")).append(',');
            sb.append(obj.getString("adminPort")).append(',');
            sb.append(obj.getString("httpPort"));
            return sb.toString();
        } catch (Exception e) {
            hostLog.warn("Unable to format host metadata " + json, e);
        }
        return "";
    }

    public static boolean isPro() {
        return null != MiscUtils.loadProClass("org.voltdb.CommandLogImpl", "Command logging", true);
    }
}
