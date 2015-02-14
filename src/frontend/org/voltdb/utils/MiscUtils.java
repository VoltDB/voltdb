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

package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ReplicationRole;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.licensetool.LicenseException;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.ListMultimap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Multimaps;
import com.google_voltpatches.common.net.HostAndPort;

public class MiscUtils {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private static final String licenseFileName = "license.xml";

    /**
     * Simple code to copy a file from one place to another...
     * Java should have this built in... stupid java...
     */
    public static void copyFile(String fromPath, String toPath) throws Exception {
        File inputFile = new File(fromPath);
        File outputFile = new File(toPath);
        com.google_voltpatches.common.io.Files.copy(inputFile, outputFile);
    }

    /**
     * Serialize a file into bytes. Used to serialize catalog and deployment
     * file for UpdateApplicationCatalog on the client.
     *
     * @param path
     * @return a byte array of the file
     * @throws IOException
     *             If there are errors reading the file
     */
    public static byte[] fileToBytes(File path) throws IOException {
        FileInputStream fin = new FileInputStream(path);
        byte[] buffer = new byte[(int) fin.getChannel().size()];
        try {
            if (fin.read(buffer) == -1) {
                throw new IOException("File " + path.getAbsolutePath() + " is empty");
            }
        } finally {
            fin.close();
        }
        return buffer;
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
            return null;
        }

        // Initialize the API. This parses the file but does NOT verify signatures.
        if (licenseApi.initializeFromFile(licenseFile) == false) {
            hostLog.fatal("Unable to load license file: could not parse license.");
            return null;
        }

        // Perform signature verification - detect modified files
        try
        {
            if (licenseApi.verify() == false) {
                hostLog.fatal("Unable to load license file: could not verify license signature.");
                return null;
            }
        }
        catch (LicenseException lex)
        {
            hostLog.fatal(lex.getMessage());
            return null;
        }

        return licenseApi;
    }

    /**
     * Instantiate the license api impl based on enterprise/community editions
     * For enterprise edition, look in default locations ./, ~/, jar file directory
     * @return a valid API for community and pro editions, or null on error.
     */
    public static LicenseApi licenseApiFactory()
    {
        String licensePath = System.getProperty("user.dir") + "/" + licenseFileName;
        LicenseApi licenseApi = MiscUtils.licenseApiFactory(licensePath);
        if (licenseApi == null) {
            try {
                // Get location of jar file
                String jarLoc = VoltDB.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                // Strip of file name
                int lastSlashOff = jarLoc.lastIndexOf("/");
                if (lastSlashOff == -1) {
                    // Jar is at root directory
                    licensePath = "/" + licenseFileName;
                }
                else {
                    licensePath = jarLoc.substring(0, lastSlashOff+1) + licenseFileName;
                }
                licenseApi = MiscUtils.licenseApiFactory(licensePath);
            }
            catch (URISyntaxException e) {
            }
        }
        if (licenseApi == null) {
            licensePath = System.getProperty("user.home") + "/" + licenseFileName;
            licenseApi = MiscUtils.licenseApiFactory(licensePath);
        }
        if (licenseApi != null) {
            hostLog.info("Searching for license file located " + licensePath);
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
                hostLog.fatal("Please contact sales@voltdb.com to request a new license.");
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
     * there are exactly three parts in the version. Each part must be separated
     * by a dot.
     *
     * @param versionString
     * @return an array of each part as integer.
     */
    public static Object[] parseVersionString(String versionString) {
        if (versionString == null) {
            return null;
        }

        // check for whitespace
        if (versionString.matches("\\s")) {
            return null;
        }

        // split on the dots
        String[] split = versionString.split("\\.");
        if (split.length == 0) {
            return null;
        }

        Object[] v = new Object[split.length];
        int i = 0;
        for (String s : split) {
            try {
                v[i] = Integer.parseInt(s);
            } catch (NumberFormatException e) {
                v[i] = s;
            }
            i++;
        }

        // check for a numeric beginning
        if (v[0] instanceof Integer) {
            return v;
        }
        else {
            return null;
        }
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
    public static int compareVersions(Object[] left, Object[] right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Invalid versions");
        }

        for (int i = 0; i < left.length; i++) {
            // right is shorter than left and share the same prefix => left must be larger
            if (right.length == i) {
                return 1;
            }

            if (left[i] instanceof Integer) {
                if (right[i] instanceof Integer) {
                    // compare two numbers
                    if (((Integer) left[i]) > ((Integer) right[i])) {
                        return 1;
                    } else if (((Integer) left[i]) < ((Integer) right[i])) {
                        return -1;
                    }
                    else {
                        continue;
                    }
                }
                else {
                    // numbers always greater than alphanumeric tags
                    return 1;
                }
            }
            else if (right[i] instanceof Integer) {
                // alphanumeric tags always less than numbers
                return -1;
            }
            else {
                // compare two alphanumeric tags lexicographically
                int cmp = ((String) left[i]).compareTo((String) right[i]);
                if (cmp != 0) {
                    return cmp;
                }
                else {
                    // two alphanumeric tags are the same... so keep comparing
                    continue;
                }
            }
        }

        // left is shorter than right and share the same prefix, must be less
        if (left.length < right.length) {
            return -1;
        }

        // samesies
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

    // cache whether we're running pro code
    private static Boolean m_isPro = null;
    // check if we're running pro code
    public static boolean isPro() {
        if (m_isPro == null) {
            m_isPro = null != MiscUtils.loadProClass("org.voltdb.CommandLogImpl", "Command logging", true);
        }
        return m_isPro.booleanValue();
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
     * @return HostAndPort number.
     */
    public static HostAndPort getHostAndPortFromHostnameColonPort(String server, int defaultPort) {
        return HostAndPort.fromString(server).withDefaultPort(defaultPort);
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
            /*
             * Don't do DNS resolution, don't use names for port numbers
             */
            ProcessBuilder pb = new ProcessBuilder("lsof", "-i", "-n", "-P");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            java.io.InputStreamReader reader = new java.io.InputStreamReader(p.getInputStream());
            java.io.BufferedReader br = new java.io.BufferedReader(reader);
            String str = br.readLine();
            log.fatal("Logging ports that are bound for listening, " +
                      "this doesn't include ports bound by outgoing connections " +
                      "which can also cause a failure to bind");
            log.fatal("The PID of this process is " + CLibrary.getpid());
            if (str != null) {
                log.fatal(str);
            }
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
     * Get the resident set size, in mb, for the voltdb server on the other end of the client.
     * If the client is connected to multiple servers, return the max individual rss across
     * the cluster.
     */
    public static long getMBRss(Client client) {
        assert(client != null);
        long rssMax = 0;
        try {
            ClientResponse r = client.callProcedure("@Statistics", "MEMORY", 0);
            VoltTable stats = r.getResults()[0];
            stats.resetRowPosition();
            while (stats.advanceRow()) {
                long rss = stats.getLong("RSS") / 1024;
                if (rss > rssMax) {
                    rssMax = rss;
                }
            }
            return rssMax;
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return 0;
        }
    }

    /**
     * Zip the two lists up into a multimap
     * @return null if one of the lists is empty
     */
    public static <K, V> Multimap<K, V> zipToMap(List<K> keys, List<V> values)
    {
        if (keys.isEmpty() || values.isEmpty()) {
            return null;
        }

        Iterator<K> keyIter = keys.iterator();
        Iterator<V> valueIter = values.iterator();
        ArrayListMultimap<K, V> result = ArrayListMultimap.create();

        while (keyIter.hasNext() && valueIter.hasNext()) {
            result.put(keyIter.next(), valueIter.next());
        }

        // In case there are more values than keys, assign the rest of the
        // values to the first key
        K firstKey = keys.get(0);
        while (valueIter.hasNext()) {
            result.put(firstKey, valueIter.next());
        }

        return result;
    }

    /**
     * Create an ArrayListMultimap that uses TreeMap as the container map, so order is preserved.
     */
    public static <K extends Comparable, V> ListMultimap<K, V> sortedArrayListMultimap()
    {
        Map<K, Collection<V>> map = Maps.newTreeMap();
        return Multimaps.newListMultimap(map, new Supplier<List<V>>() {
            @Override
            public List<V> get()
            {
                return Lists.newArrayList();
            }
        });
    }

    /**
     * Serialize and then deserialize an invocation so that it has serializedParams set for command logging if the
     * invocation is sent to a local site.
     * @return The round-tripped version of the invocation
     * @throws IOException
     */
    public static StoredProcedureInvocation roundTripForCL(StoredProcedureInvocation invocation) throws IOException
    {
        if (invocation.getSerializedParams() == null) {
            ByteBuffer buf = ByteBuffer.allocate(invocation.getSerializedSize());
            invocation.flattenToBuffer(buf);
            buf.flip();

            StoredProcedureInvocation rti = new StoredProcedureInvocation();
            rti.initFromBuffer(buf);
            return rti;
        } else {
            return invocation;
        }
    }

    /**
     * Utility class to convert and hold a human-friendly time value and unit
     * string.  For now it only deals with hours, minutes or seconds and their
     * fractions.
     * TODO: Parameterize conversion to optionally support other units, e.g. ms.
     */
    public static class HumanTime
    {
        /// The scaled time value.
        public final double value;
        /// The scale unit name ("hour", "minute", or "second").
        public final String unit;

        /**
         * Private constructor. Use static methods to construct.
         * @param value the scaled time value.
         * @param unit the unit name (unchecked)
         */
        private HumanTime(double value, String unit)
        {
            this.value = value;
            this.unit = unit;
        }

        /**
         * Scale a nanoseconds number for human consumption.
         * @param nanos time in nanoseconds.
         */
        public static HumanTime scale(double nanos)
        {
            // Start with hours and adjust down until it's >1. Stop at seconds.
            double value = nanos / 1000000000 / 3600;
            String unit;
            if (value >= 1) {
                unit = "hour";
            }
            else{
                value *= 60.0;
                if (value >= 1) {
                    unit = "minute";
                }
                else {
                    value *= 60.0;
                    unit = "second";
                }
            }
            return new HumanTime(value, unit);
        }

        /**
         * Format a string for human consumption based on raw nanoseconds.
         * @param nanos time in nanoseconds.
         * @return formatted string.
         */
        public static String formatTime(double nanos)
        {
            HumanTime tu = scale(nanos);
            return String.format("%.2f %ss", tu.value, tu.unit);
        }

        /**
         * Format a rate string, for example <value>/second based on an input value
         * and duration in nanoseconds. Specify the itemUnit value if you would
         * like to insert a character or word (add your own leading space),
         * e.g. "%" or " Megawatts", between the rate and the slash ('/').
         * @param value arbitrary value for rate calculation.
         * @param nanos time in nanoseconds.
         * @param itemUnit unit name for value
         * @return formatted string.
         */
        public static String formatRate(double value, double nanos, String itemUnit)
        {
            // Multiply by 60 so that a seconds duration becomes a per minute rate, and so on..
            HumanTime tu = scale((nanos * 60) / value);
            return String.format("%.2f%s/%s", 60 / tu.value, itemUnit, tu.unit);
        }

        /**
         * Format a rate string, for example <value>/second based on an input value
         * and duration in nanoseconds.
         * @param value arbitrary value for rate calculation.
         * @param nanos time in nanoseconds.
         * @return formatted string.
         */
        public static String formatRate(double value, double nanos)
        {
            return formatRate(value, nanos, "");
        }
    }

    public static String formatUptime(long uptimeInMs)
    {
        long remainingMs = uptimeInMs;
        long days = TimeUnit.MILLISECONDS.toDays(remainingMs);
        remainingMs -= TimeUnit.DAYS.toMillis(days);
        long hours = TimeUnit.MILLISECONDS.toHours(remainingMs);
        remainingMs -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(remainingMs);
        remainingMs -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(remainingMs);
        remainingMs -= TimeUnit.SECONDS.toMillis(seconds);
        return String.format("%d days %02d:%02d:%02d.%03d",
                days, hours, minutes, seconds, remainingMs);
    }

    /**
     * Delays retrieval until first use, but holds onto a boolean value to
     * minimize overhead. The delayed retrieval allows tests to set properties
     * dynamically and have them obeyed.
     */
    public static class BooleanSystemProperty
    {
        private final String key;
        private Boolean value = null;
        private final boolean defaultValue;

        /**
         * Construct system property retriever with default value of false
         * @param key  key name
         */
        public BooleanSystemProperty(String key)
        {
            this(key, false);
        }

        /**
         * Construct system property retriever with default value provided by caller
         * @param key  key name
         */
        public BooleanSystemProperty(String key, boolean defaultValue)
        {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * Retrieves once and caches boolean value. Uses default if not available.
         * @return true if value or default is true ("true" or "yes" string)
         */
        public boolean isTrue()
        {
            if (this.value == null) {
                // First time - retrieve and convert the value or use the default value.
                String stringValue = System.getProperty(this.key);
                if (stringValue != null) {
                    this.value = (stringValue.equalsIgnoreCase("true") || stringValue.equalsIgnoreCase("yes"));
                }
                else {
                    this.value = this.defaultValue;
                }
            }
            assert this.value != null;
            return this.value;
        }
    }
}
