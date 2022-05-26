/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.FileReader;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.RealVoltDB;
import org.voltdb.StartAction;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TxnEgo;

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

    private static final boolean assertsEnabled;

    static {
        boolean assertCaught = false;
        assert(assertCaught = true);
        assertsEnabled = assertCaught;
    }

    public static boolean areAssertsEnabled() {
        return assertsEnabled;
    }

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
     * Notice that if the file is larger than 2GB, readAllBytes() will throw a OutOfMemoryError.
     *
     * @param path
     * @return a byte array of the file
     * @throws IOException
     *             If there are errors reading the file
     */
    public static byte[] fileToBytes(File path) throws IOException {
        return Files.readAllBytes(path.toPath());
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
        if (! m.find()) {
            return null;
        }
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

    // Cache whether we're running Pro code.
    private static Boolean m_isPro = null;

    // Check for Pro build.
    public static boolean isPro() {
        if (m_isPro == null) {
            m_isPro = ProClass.load("org.voltdb.CommandLogImpl", "Command logging", ProClass.HANDLER_IGNORE)
                              .hasProClass();
            // It used to be possible to pretend Pro code was Community, but that
            // hasn't worked correctly since about V10. Ignore but warn.
            if (m_isPro && Boolean.parseBoolean(System.getProperty("community", "false"))) {
                hostLog.warn("Property 'community' is set but has no effect on Pro code");
            }
        }
        return m_isPro.booleanValue();
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     *               IPv6 addresses must be enclosed in brackets.
     * @return hostname or textual ip representation.
     *         IPv6 address will not have brackets.
     */
    public static String getHostnameFromHostnameColonPort(String server) {
        return HostAndPort.fromString(server).requireBracketsForIPv6().getHostText();
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     *               IPv6 addresses must be enclosed in brackets.
     * @param defaultPort If a port isn't specified, use this one.
     * @return port number.
     */
    public static int getPortFromHostnameColonPort(String server, int defaultPort) {
        return HostAndPort.fromString(server).requireBracketsForIPv6().getPortOrDefault(defaultPort);
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     *               IPv6 addresses must be enclosed in brackets.
     * @param defaultPort If a port isn't specified, use this one.
     * @return HostAndPort number.
     */
    public static HostAndPort getHostAndPortFromHostnameColonPort(String server, int defaultPort) {
        return HostAndPort.fromString(server).withDefaultPort(defaultPort).requireBracketsForIPv6();
    }

    /**
     * @param server String containing a hostname/ip, or a hostname/ip:port.
     *               IPv6 addresses must be enclosed in brackets.
     * @param defaultPort If a port isn't specified, use this one.
     * @return String in hostname/ip:port format.
     *         IPv6 address will be enclosed in brackets.
     */
    public static String getHostnameColonPortString(String server, int defaultPort) {
        return getHostAndPortFromHostnameColonPort(server, defaultPort).toString();
    }

    /**
     * Used to parse many of the --fubar=[interface][:port] command options.
     * This differs from the other 'host and port' methods in that it treats
     * a spec that consists only of decimal digits as a port number. Note that
     * brackets are required around IPv6 addresses to remove ambiguity about
     * whether there is a port number.
     *
     * Expected forms: (brackets are literal here)
     *    portnum
     *    hostname       hostname:portnum
     *    ip4address    ip4address:portnum
     *    [ip6address]  [ip6address]:portnum
     *
     * @param spec one of: port, hostname, hostname:port, ipaddr, ipaddr:port
     * @param defaultHost: if spec is port number, this is used as the host
     * @param defaultPort: if a port isn't specified, use this
     * @return HostAndPort number.
     * @throws IllegalArgumentException
     */
    public static HostAndPort getHostAndPortFromInterfaceSpec(String spec, String defaultHost, int defaultPort) {
        spec = (spec == null ? "" : spec.trim());
        if (spec.matches("^[0-9]+$")) {
            return HostAndPort.fromParts(defaultHost, Integer.parseInt(spec));
        }
        else {
            return HostAndPort.fromString(spec).withDefaultPort(defaultPort).requireBracketsForIPv6();
        }
    }

    /**
     * Handles interface addresses that are not supposed to contain a port
     * number (which is checked). Strips brackets that may be around an
     * IPv6 address, returning the bare address.
     *
     * @param host - one of hostname, ip4 address, ip6 address, [ip6 address]
     * @return input with brackets removed
     * @throws IllegalArgumentException
     */
    public static String getAddressOfInterface(String host) {
        host = (host == null ? "" : host.trim());
        return HostAndPort.fromHost(host).getHost();
    }

    /**
     * Combines an address/hostname and port number into an "interface spec",
     * taking care of adding brackets as necessary for IPv6 addresses.
     * Both host and port are required here.
     *
     * @param host - one of hostname, ip4 address, ip6 address
     * @param port - port number
     * @return host and port as string
     */
    public static String makeInterfaceSpec(String host, int port) {
        host = (host == null ? "" : host.trim());
        if (host.contains(":") && host.charAt(0) != '[') {
            host = "[" + host + "]";
        }
        return host + ":" + port;
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
     * Concatenate an list of arrays of typed-objects
     * @param empty An empty array of the right type used for cloning
     * @param arrayList A list of arrays to concatenate.
     * @return The concatenated mega-array.
     */
    public static <T> T[] concatAll(final T[] empty, Iterable<T[]> arrayList) {
        assert(empty.length == 0);
        if (arrayList.iterator().hasNext() == false) {
            return empty;
        }

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

    public static boolean deleteRecursively( File file) {
        if (file == null || !file.exists() || !file.canRead() || !file.canWrite()) {
            return false;
        }
        if (file.isDirectory() && file.canExecute()) {
            for (File f: file.listFiles()) {
                deleteRecursively(f);
            }
        }
        return file.delete();
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
     * Aggregates the elements from each of the given deque. It takes one
     * element from the head of each deque in each loop and put them into a
     * single list. This method modifies the deques in-place.
     * @param stuff
     * @return
     */
    public static <K> List<K> zip(Collection<Deque<K>> stuff)
    {
        final List<K> result = Lists.newArrayList();

        // merge the results
        Iterator<Deque<K>> iter = stuff.iterator();
        while (iter.hasNext()) {
            final K next = iter.next().poll();
            if (next != null) {
                result.add(next);
            } else {
                iter.remove();
            }

            if (!iter.hasNext()) {
                iter = stuff.iterator();
            }
        }

        return result;
    }

    /**
     * Create an ArrayListMultimap that uses TreeMap as the container map, so order is preserved.
     */
    public static <K extends Comparable<?>, V> ListMultimap<K, V> sortedArrayListMultimap()
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
        if (invocation.getSerializedParams() != null) {
            return invocation;
        }
        ByteBuffer buf = ByteBuffer.allocate(invocation.getSerializedSize());
        invocation.flattenToBuffer(buf);
        buf.flip();

        StoredProcedureInvocation rti = new StoredProcedureInvocation();
        rti.initFromBuffer(buf);
        return rti;
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
        if (uptimeInMs < 0) {
            return "unknown";
        }
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

    public static String hsIdTxnIdToString(long hsId, long txnId) {
        final StringBuilder sb = new StringBuilder();
        CoreUtils.hsIdToString(hsId, sb);
        sb.append(" ");
        TxnEgo.txnIdToString(txnId, sb);
        return sb.toString();
    }

    public static String hsIdPairTxnIdToString(final long srcHsId, final long destHsId,
                                               final long txnId, final long uniqID) {
        final StringBuilder sb = new StringBuilder(32);
        CoreUtils.hsIdToString(srcHsId, sb);
        sb.append("->");
        CoreUtils.hsIdToString(destHsId, sb);
        sb.append(" ");
        TxnEgo.txnIdToString(txnId, sb);
        sb.append(" ").append(uniqID);
        return sb.toString();
    }

    /**
     * Get VARBINARY partition keys for the current topology.
     * @return A map from partition IDs to partition keys, null if failed to get the keys.
     */
    public static Map<Integer, byte[]> getBinaryPartitionKeys() {
        return getBinaryPartitionKeys(null);
    }

    /**
     * Get VARBINARY partition keys for the specified topology.
     * @return A map from partition IDs to partition keys, null if failed to get the keys.
     */
    public static Map<Integer, byte[]> getBinaryPartitionKeys(TheHashinator hashinator) {
        Map<Integer, byte[]> partitionMap = new HashMap<>();

        VoltTable partitionKeys = null;
        if (hashinator == null) {
            partitionKeys = TheHashinator.getPartitionKeys(VoltType.VARBINARY);
        }
        else {
            partitionKeys = TheHashinator.getPartitionKeys(hashinator, VoltType.VARBINARY);
        }
        if (partitionKeys == null) {
            return null;
        } else {
            // This is a shared resource so make a copy of the table to protect the cache copy in TheHashinator
            ByteBuffer buf = ByteBuffer.allocate(partitionKeys.getSerializedSize());
            partitionKeys.flattenToBuffer(buf);
            buf.flip();
            VoltTable keyCopy = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
            while (keyCopy.advanceRow()) {
                partitionMap.put((int) keyCopy.getLong(0), keyCopy.getVarbinary(1));
            }
        }
        return partitionMap;
    }

    /**
     * Get username and password from credentials file.
     * @return a Properties variable which contains username and password.
     */
    public static Properties readPropertiesFromCredentials(String credentials) {
        Properties props = new Properties();
        File propFD = new File(credentials);
        if (!propFD.exists() || !propFD.isFile() || !propFD.canRead()) {
            throw new IllegalArgumentException("Credentials file " + credentials + " is not a read accessible file");
        } else {
            FileReader fr = null;
            try {
                fr = new FileReader(credentials);
                props.load(fr);
            } catch (IOException e) {
                throw new IllegalArgumentException("Credential file not found or permission denied.");
            }
        }
        return props;
    }

    /**
     * Serialize the deferred serializer data into byte buffer
     * @param mbuf ByteBuffer the buffer is written to
     * @param ds DeferredSerialization data writes to the byte buffer
     * @return size of data
     * @throws IOException
     */
    public static int writeDeferredSerialization(ByteBuffer mbuf, DeferredSerialization ds) throws IOException
    {
        int written = 0;
        try {
            final int objStartPosition = mbuf.position();
            ds.serialize(mbuf);
            written = mbuf.position() - objStartPosition;
        } finally {
            ds.cancel();
        }
        return written;
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
}
