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
package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ReplicationRole;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.licensetool.LicenseApi;

import com.google.common.net.HostAndPort;

public class MiscUtils {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    /**
     * Simple code to copy a file from one place to another...
     * Java should have this built in... stupid java...
     */
    public static void copyFile(String fromPath, String toPath) throws Exception {
        File inputFile = new File(fromPath);
        File outputFile = new File(toPath);
        com.google.common.io.Files.copy(inputFile, outputFile);
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
     * Instantiate the license api impl based on enterprise/community editions
     * @return a valid API for community and pro editions, or null on error.
     */
    public static LicenseApi licenseApiFactory(String pathToLicense) {

        if (MiscUtils.isPro() == false) {
            return new LicenseApi() {
                @Override
                public boolean initializeFromFile(File license) {
                    return true;
                }

                @Override
                public boolean isTrial() {
                    return false;
                }

                @Override
                public int maxHostcount() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public Calendar expires() {
                    Calendar result = Calendar.getInstance();
                    result.add(Calendar.YEAR, 20); // good enough?
                    return result;
                }

                @Override
                public boolean verify() {
                    return true;
                }

                @Override
                public boolean isDrReplicationAllowed() {
                    return false;
                }

                @Override
                public boolean isCommandLoggingAllowed() {
                    return false;
                }
            };
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
                return null;
            } catch (IllegalAccessException e) {
                hostLog.fatal("Unable to process license file: could not create license API.");
                return null;
            }
        }

        if (licenseApi == null) {
            hostLog.fatal("Unable to load license file: could not create License API.");
            return null;
        }

        // verify the license file exists.
        File licenseFile = new File(pathToLicense);
        if (licenseFile.exists() == false) {
            hostLog.fatal("Unable to open license file: " + pathToLicense);
            return null;
        }

        // Initialize the API. This parses the file but does NOT verify signatures.
        if (licenseApi.initializeFromFile(licenseFile) == false) {
            hostLog.fatal("Unable to load license file: could not parse license.");
            return null;
        }

        // Perform signature verification - detect modified files
        if (licenseApi.verify() == false) {
            hostLog.fatal("Unable to load license file: could not verify license signature.");
            return null;
        }

        return licenseApi;
    }

    /**
     * Validate the signature and business logic enforcement for a license.
     * @return true if the licensing constraints are met
     */
    public static boolean validateLicense(LicenseApi licenseApi,
            int numberOfNodes, ReplicationRole replicationRole)
    {
        // Delay the handling of an invalid license file until here so
        // that the leader can terminate the full cluster.
        if (licenseApi == null) {
            hostLog.fatal("VoltDB license is not valid.");
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

        // enforce DR replication constraint
        if (replicationRole == ReplicationRole.REPLICA) {
            if (licenseApi.isDrReplicationAllowed() == false) {
                hostLog.fatal("Warning, VoltDB license does not allow use of DR replication.");
                return false;
            }
        }

        // print out trial success message
        if (licenseApi.isTrial()) {
            consoleLog.info("Starting VoltDB with trial license. License expires on " + expiresStr + ".");
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
        consoleLog.info(msg);

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

    /**
     * Parse a version string in the form of x.y.z. It doesn't require that
     * there are exactly three parts in the version. Each part must be seperated
     * by a dot.
     *
     * @param versionString
     * @return an array of each part as integer.
     */
    public static int[] parseVersionString(String versionString) {
        if (versionString == null) {
            return null;
        }

        String[] split = versionString.split("\\.");
        int[] v = new int[split.length];
        int i = 0;
        for (String s : split) {
            try {
                v[i++] = Integer.parseInt(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return v;
    }

    /**
     * Compare two versions. Version should be represented as an array of
     * integers.
     *
     * @param left
     * @param right
     * @return -1 if left is smaller than right, 0 if they are equal, 1 if left
     *         is greater than right.
     */
    public static int compareVersions(int[] left, int[] right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Invalid versions");
        }

        int i = 0;
        for (int part : right) {
            // left is shorter than right and share the same prefix, must be smaller
            if (left.length == i) {
                return -1;
            }

            if (left[i] > part) {
                return 1;
            } else if (left[i] < part) {
                return -1;
            }

            i++;
        }

        // left is longer than right and share the same prefix, must be greater
        if (left.length > i) {
            return 1;
        }
        return 0;
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

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     * @param defaultPort If a port isn't specified, use this one.
     * @return hostname or textual ip representation.
     */
    public static String getHostnameFromHostnameColonPort(String server) {
        return HostAndPort.fromString(server).getHostText();
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     * @param defaultPort If a port isn't specified, use this one.
     * @return port number.
     */
    public static int getPortFromHostnameColonPort(String server, int defaultPort) {
        return HostAndPort.fromString(server).getPortOrDefault(defaultPort);
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     * @param defaultPort If a port isn't specified, use this one.
     * @return String in hostname/ip:port format.
     */
    public static String getHostnameColonPortString(String server, int defaultPort) {
        return HostAndPort.fromString(server).withDefaultPort(defaultPort).toString();
    }

    /**
     * I heart commutativity
     * @param buffer ByteBuffer assumed position is at end of data
     * @return the cheesy checksum of this VoltTable
     */
    public static final long cheesyBufferCheckSum(ByteBuffer buffer) {
        final int mypos = buffer.position();
        buffer.position(0);
        long checksum = 0;
        if (buffer.hasArray()) {
            final byte bytes[] = buffer.array();
            final int end = buffer.arrayOffset() + mypos;
            for (int ii = buffer.arrayOffset(); ii < end; ii++) {
                checksum += bytes[ii];
            }
        } else {
            for (int ii = 0; ii < mypos; ii++) {
                checksum += buffer.get();
            }
        }
        buffer.position(mypos);
        return checksum;
    }

    public static String getCompactStringTimestamp(long timestamp) {
        SimpleDateFormat sdf =
                new SimpleDateFormat("MMddHHmmss");
        Date tsDate = new Date(timestamp);
        return sdf.format(tsDate);
    }

    public static synchronized boolean isBindable(int port) {
        try {
            ServerSocket ss = new ServerSocket(port);
            ss.close();
            ss = null;
            return true;
        }
        catch (BindException be) {
            return false;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Log (to the fatal logger) the list of ports in use.
     * Uses "lsof -i" internally.
     *
     * @param log VoltLogger used to print output or warnings.
     */
    public static synchronized void printPortsInUse(VoltLogger log) {
        try {
            Process p = Runtime.getRuntime().exec("lsof -i");
            java.io.InputStreamReader reader = new java.io.InputStreamReader(p.getInputStream());
            java.io.BufferedReader br = new java.io.BufferedReader(reader);
            String str = null;
            while((str = br.readLine()) != null) {
                if (str.contains("LISTEN")) {
                    log.fatal(str);
                }
            }
        }
        catch (Exception e) {
            log.fatal("Unable to list ports in use at this time.");
        }
    }

    /**
     * Split SQL statements on semi-colons with quoted string and comment support.
     *
     * Degenerate formats such as escape as the last character or unclosed strings are ignored and
     * left to the SQL parser to complain about. This is a simple string splitter that errs on the
     * side of not splitting.
     *
     * Regexes are avoided.
     *
     * Handle single and double quoted strings and backslash escapes. Backslashes escape a single
     * character.
     *
     * Handle double-dash (single line) and C-style (muli-line) comments. Nested C-style comments
     * are not supported.
     *
     * @param sql raw SQL text to split
     * @return list of individual SQL statements
     */
    public static List<String> splitSQLStatements(final String sql) {
        List<String> statements = new ArrayList<String>();
        // Use a character array for efficient character-at-a-time scanning.
        char[] buf = sql.toCharArray();
        // Set to null outside of quoted segments or the quote character inside them.
        Character cQuote = null;
        // Set to null outside of comments or to the string that ends the comment.
        String sCommentEnd = null;
        // Index to start of current statement.
        int iStart = 0;
        // Index to current character.
        // IMPORTANT: The loop is structured in a way that requires all if/else/... blocks to bump
        // iCur appropriately. Failure of a corner case to bump iCur will cause an infinite loop.
        int iCur = 0;
        while (iCur < buf.length) {
            if (sCommentEnd != null) {
                // Processing the interior of a comment. Check if at the comment or buffer end.
                if (iCur >= buf.length - sCommentEnd.length()) {
                    // Exit
                    iCur = buf.length;
                } else if (String.copyValueOf(buf, iCur, sCommentEnd.length()).equals(sCommentEnd)) {
                    // Move past the comment end.
                    iCur += sCommentEnd.length();
                    sCommentEnd = null;
                } else {
                    // Keep going inside the comment.
                    iCur++;
                }
            } else if (cQuote != null) {
                // Processing the interior of a quoted string.
                if (buf[iCur] == '\\') {
                    // Skip the '\' escape and the trailing single escaped character.
                    // Doesn't matter if iCur is beyond the end, it won't be used in that case.
                    iCur += 2;
                } else if (buf[iCur] == cQuote) {
                    // Look at the next character to distinguish a double escaped quote
                    // from the end of the quoted string.
                    iCur++;
                    if (iCur < buf.length) {
                        if (buf[iCur] != cQuote) {
                            // Not a double escaped quote - end of quoted string.
                            cQuote = null;
                        } else {
                            // Move past the double escaped quote.
                            iCur++;
                        }
                    }
                } else {
                    // Move past an ordinary character.
                    iCur++;
                }
            } else {
                // Outside of a quoted string - watch for the next separator, quote or comment.
                if (buf[iCur] == ';') {
                    // Add terminated statement (if not empty after trimming).
                    String statement = String.copyValueOf(buf, iStart, iCur - iStart).trim();
                    if (!statement.isEmpty()) {
                        statements.add(statement);
                    }
                    iStart = iCur + 1;
                    iCur = iStart;
                } else if (buf[iCur] == '"' || buf[iCur] == '\'') {
                    // Start of quoted string.
                    cQuote = buf[iCur];
                    iCur++;
                } else if (iCur <= buf.length - 2) {
                    // Comment (double-dash or C-style)?
                    if (buf[iCur] == '-' && buf[iCur+1] == '-') {
                        // One line double-dash comment start.
                        sCommentEnd = "\n"; // Works for *IX (\n) and Windows (\r\n).
                        iCur += 2;
                    } else if (buf[iCur] == '/' && buf[iCur+1] == '*') {
                        // Multi-line C-style comment start.
                        sCommentEnd = "*/";
                        iCur += 2;
                    } else {
                        // Not a comment start, move past this character.
                        iCur++;
                    }
                } else {
                    // Move past a non-quote/non-separator character.
                    iCur++;
                }
            }
        }
        // Get the last statement, if any.
        if (iStart < buf.length) {
            String statement = String.copyValueOf(buf, iStart, iCur - iStart).trim();
            if (!statement.isEmpty()) {
                statements.add(statement);
            }
        }
        return statements;
    }

    /**
     * Concatenate an list of arrays of typed-objects
     * @param empty An empty array of the right type used for cloning
     * @param arrayList A list of arrays to concatenate.
     * @return The concatenated mega-array.
     */
    public static <T> T[] concatAll(final T[] empty, Iterable<T[]> arrayList) {
        assert(empty.length == 0);
        if (arrayList.iterator().hasNext() == false) return empty;

        int len = 0;
        for (T[] subArray : arrayList) {
            len += subArray.length;
        }
        int pos = 0;
        T[] result = Arrays.copyOf(empty, len);
        for (T[] subArray : arrayList) {
            System.arraycopy(subArray, 0, result, pos, subArray.length);
            pos += subArray.length;
        }
        return result;
    }

    public static void deleteRecursively( File file) {
        if (file == null || !file.exists() || !file.canRead() || !file.canWrite()) return;
        if (file.isDirectory() && file.canExecute()) {
            for (File f: file.listFiles()) {
                deleteRecursively(f);
            }
        }
        file.delete();
    }

    /**
     * Get the resident set size, in mb for the voltdb server on the other end of the client
     */
    public static long getMBRss(Client client) {
        assert(client != null);
        try {
            ClientResponse r = client.callProcedure("@Statistics", "MEMORY", 0);
            VoltTable stats = r.getResults()[0];
            stats.advanceRow();
            long rss = stats.getLong("RSS") / 1024;
            return rss;
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return 0;
        }
    }
}
