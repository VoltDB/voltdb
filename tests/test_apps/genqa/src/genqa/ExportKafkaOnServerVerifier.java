/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
package genqa;

import genqa.procedures.SampleRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.voltdb.VoltDB;
import org.voltdb.types.TimestampType;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ExportKafkaOnServerVerifier {

    public static long VALIDATION_REPORT_INTERVAL = 50000;

    private final List<RemoteHost> m_hosts = new ArrayList<RemoteHost>();

    private static class RemoteHost {
        @SuppressWarnings("unused")
        String host;
        String port;
        ConsumerConfig consumerConfig;
        ConsumerConnector consumer;
        boolean activeSeen = false;
        boolean fileSeen = false;

        public void buildConfig(String a_zookeeper) {
            Properties props = new Properties();
            props.put("zookeeper.connect", a_zookeeper);
            props.put("group.id", "exportverifier");
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.commit.enable", "true");
            props.put("auto.offset.reset", "smallest");

            consumerConfig = new ConsumerConfig(props);

            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
            try {
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                Logger.getLogger(ExportKafkaOnServerVerifier.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        public void stop() {
            consumer.commitOffsets();
            consumer.shutdown();
        }
    }

    public static class ValidationErr extends Exception {
        private static final long serialVersionUID = 1L;
        final String msg;
        final Object value;
        final Object expected;

        ValidationErr(String msg, Object value, Object expected) {
            this.msg = msg;
            this.value = value;
            this.expected = expected;
        }

        public ValidationErr(String string) {
            this.msg = string;
            this.value = "[not provided]";
            this.expected = "[not provided]";
        }
        @Override
        public String toString() {
            return msg + " Value: " + value + " Expected: " + expected;
        }
    }

    ExportKafkaOnServerVerifier()
    {
    }

    int splitClientTrace(String trace, long [] splat)
    {
        if (trace == null || splat == null || splat.length == 0) return 0;

        int columnCount = 0;
        int cursor = 0;
        int columnPos = trace.indexOf(':', cursor);

        try {
            while (columnPos >= 0 && splat.length > columnCount+1) {
                splat[columnCount] = Long.parseLong(trace.substring(cursor, columnPos));
                cursor = columnPos + 1;
                columnCount = columnCount + 1;
                columnPos = trace.indexOf(':', cursor);
            }
            if (cursor < trace.length()) {
                columnPos = columnPos < 0 ? trace.length() : columnPos;
                splat[columnCount] = Long.parseLong(trace.substring(cursor, columnPos));
            } else {
                columnCount = columnCount - 1;
            }
        } catch (NumberFormatException nfex) {
            return 0;
        } catch (IndexOutOfBoundsException ioobex) {
            return -1;
        }

        return columnCount+1;
    }

    private long unquoteLong(String quoted)
    {
        StringBuilder sb = new StringBuilder(quoted);
        int i = 0;
        while (i < sb.length()) {
            if (sb.charAt(i) == '"') sb.deleteCharAt(i);
            else ++i;
        }
        return Long.parseLong(sb.toString().trim());
    }

    int splitCSV(String csv, long [] splat)
    {
        if (csv == null || splat == null || splat.length == 0) return 0;

        int columnCount = 0;
        int cursor = 0;
        int columnPos = csv.indexOf(',', cursor);

        try {
            while (columnPos >= 0 && splat.length > columnCount+1) {
                splat[columnCount] = unquoteLong(csv.substring(cursor, columnPos));
                cursor = columnPos + 1;
                columnCount = columnCount + 1;
                columnPos = csv.indexOf(',', cursor);
            }
            if (cursor < csv.length()) {
                columnPos = columnPos < 0 ? csv.length() : columnPos;
                splat[columnCount] = unquoteLong(csv.substring(cursor, columnPos));
            } else {
                columnCount = columnCount - 1;
            }
        } catch (NumberFormatException nfex) {
            return 0;
        } catch (IndexOutOfBoundsException ioobex) {
            return -1;
        }

        return columnCount+1;
    }

    boolean verifySetup(String[] args) throws Exception    {
        String remoteHosts[] = args[0].split(",");

        for (String hostString : remoteHosts) {
            String split[] = hostString.split(":");
            RemoteHost rh = new RemoteHost();
            String host = split[0];
            String port = split[1];
            rh.host = host;
            rh.port = port;

            m_hosts.add(rh);
        }

        //Zookeeper
        m_zookeeper = args[1];

        //Topic
        m_topic = args[2]; //"voltdbexportEXPORT_PARTITIONED_TABLE";

        boolean skinny = false;
        if (args.length > 3 && args[3] != null && !args[3].trim().isEmpty()) {
            skinny = Boolean.parseBoolean(args[3].trim().toLowerCase());
        }
        for (RemoteHost rh : m_hosts) {
            rh.buildConfig(m_zookeeper);
        }
        return skinny;
    }

    /**
     * Verifies the fat version of the exported table. By fat it means that it contains many
     * columns of multiple types
     *
     * @throws Exception
     */
    void verifyFat() throws Exception
    {
        ExecutorService executor = openNextExportFile(m_topic);
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    /**
     * Verifies the skinny version of the exported table. By skinny it means that it contains the
     * bare minimum of columns (just enough for the purpose of transaction verification)
     *
     * @throws Exception
     */
    void verifySkinny() throws Exception
    {
        ExecutorService executor = openNextExportFile(m_topic);

        executor.awaitTermination(1, TimeUnit.DAYS);

    }

    public class ExportConsumer implements Runnable {

        private KafkaStream m_stream;
        private int m_threadNumber;

        public ExportConsumer(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                byte msg[] = it.next().message();
                System.out.println("Thread " + m_threadNumber + ": " + new String(msg));
                String row[] = RoughCSVTokenizer.tokenize(new String(msg));
                try {
                    verifyRow(row);
                } catch (ValidationErr ex) {
                    Logger.getLogger(ExportKafkaOnServerVerifier.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }

    }

    ExecutorService openNextExportFile(String topic) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(m_hosts.size() * 2);
        ExportConsumer bconsumer = null;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(3));
        for (RemoteHost rh : m_hosts) {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = rh.consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            // now launch all the threads
            //
            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                bconsumer = new ExportConsumer(stream, threadNumber++);
                executor.submit(bconsumer);
            }
        }

        return executor;
    }

    ValidationErr verifyRow(String[] row) throws ValidationErr
    {
        if (row.length < 29)
        {
            System.err.println("ERROR: Unexpected number of columns for the following row:\n\t" + Arrays.toString(row));
            return new ValidationErr("number of columns", row.length, 29);
        }

        int col = 5; // col offset is always pre-incremented.
        Long txnid = Long.parseLong(row[++col]); // col 6
        Long rowid = Long.parseLong(row[++col]); // col 7
        // matches VoltProcedure.getSeededRandomNumberGenerator()
        Random prng = new Random(txnid);
        SampleRecord valid = new SampleRecord(rowid, prng);

        // col 8
        Byte rowid_group = Byte.parseByte(row[++col]);
        if (rowid_group != valid.rowid_group)
            return error("rowid_group invalid", rowid_group, valid.rowid_group);

        // col 9
        Byte type_null_tinyint = row[++col].equals("NULL") ? null : Byte.valueOf(row[col]);
        if ( (!(type_null_tinyint == null && valid.type_null_tinyint == null)) &&
             (!type_null_tinyint.equals(valid.type_null_tinyint)) )
            return error("type_not_null_tinyint", type_null_tinyint, valid.type_null_tinyint);

        // col 10
        Byte type_not_null_tinyint = Byte.valueOf(row[++col]);
        if (!type_not_null_tinyint.equals(valid.type_not_null_tinyint))
            return error("type_not_null_tinyint", type_not_null_tinyint, valid.type_not_null_tinyint);

        // col 11
        Short type_null_smallint = row[++col].equals("NULL") ? null : Short.valueOf(row[col]);
        if ( (!(type_null_smallint == null && valid.type_null_smallint == null)) &&
             (!type_null_smallint.equals(valid.type_null_smallint)) )
            return error("type_null_smallint", type_null_smallint, valid.type_null_smallint);

        // col 12
        Short type_not_null_smallint = Short.valueOf(row[++col]);
        if (!type_not_null_smallint.equals(valid.type_not_null_smallint))
            return error("type_null_smallint", type_not_null_smallint, valid.type_not_null_smallint);

        // col 13
        Integer type_null_integer = row[++col].equals("NULL") ? null : Integer.valueOf(row[col]);
        if ( (!(type_null_integer == null && valid.type_null_integer == null)) &&
             (!type_null_integer.equals(valid.type_null_integer)) )
            return error("type_null_integer", type_null_integer, valid.type_null_integer);

        // col 14
        Integer type_not_null_integer = Integer.valueOf(row[++col]);
        if (!type_not_null_integer.equals(valid.type_not_null_integer))
            return error("type_not_null_integer", type_not_null_integer, valid.type_not_null_integer);

        // col 15
        Long type_null_bigint = row[++col].equals("NULL") ? null : Long.valueOf(row[col]);
        if ( (!(type_null_bigint == null && valid.type_null_bigint == null)) &&
             (!type_null_bigint.equals(valid.type_null_bigint)) )
            return error("type_null_bigint", type_null_bigint, valid.type_null_bigint);

        // col 16
        Long type_not_null_bigint = Long.valueOf(row[++col]);
        if (!type_not_null_bigint.equals(valid.type_not_null_bigint))
            return error("type_not_null_bigint", type_not_null_bigint, valid.type_not_null_bigint);

        // The ExportToFileClient truncates microseconds. Construct a TimestampType here
        // that also truncates microseconds.
        TimestampType type_null_timestamp;
        if (row[++col].equals("NULL")) {  // col 17
            type_null_timestamp = null;
        } else {
            TimestampType tmp = new TimestampType(row[col]);
            type_null_timestamp = new TimestampType(tmp.asApproximateJavaDate());
        }

        if ( (!(type_null_timestamp == null && valid.type_null_timestamp == null)) &&
             (!type_null_timestamp.equals(valid.type_null_timestamp)) )
        {
            System.out.println("CSV value: " + row[col]);
            System.out.println("EXP value: " + valid.type_null_timestamp.toString());
            System.out.println("ACT value: " + type_null_timestamp.toString());
            return error("type_null_timestamp", type_null_timestamp, valid.type_null_timestamp);
        }

        // col 18
        TimestampType type_not_null_timestamp = new TimestampType(row[++col]);
        if (!type_not_null_timestamp.equals(valid.type_not_null_timestamp))
            return error("type_null_timestamp", type_not_null_timestamp, valid.type_not_null_timestamp);

        // col 19
        BigDecimal type_null_decimal = row[++col].equals("NULL") ? null : new BigDecimal(row[col]);
        if ( (!(type_null_decimal == null && valid.type_null_decimal == null)) &&
             (!type_null_decimal.equals(valid.type_null_decimal)) )
            return error("type_null_decimal", type_null_decimal, valid.type_null_decimal);

        // col 20
        BigDecimal type_not_null_decimal = new BigDecimal(row[++col]);
        if (!type_not_null_decimal.equals(valid.type_not_null_decimal))
            return error("type_not_null_decimal", type_not_null_decimal, valid.type_not_null_decimal);

        // col 21
        Double type_null_float = row[++col].equals("NULL") ? null : Double.valueOf(row[col]);
        if ( (!(type_null_float == null && valid.type_null_float == null)) &&
             (!type_null_float.equals(valid.type_null_float)) )
        {
            System.out.println("CSV value: " + row[col]);
            System.out.println("EXP value: " + valid.type_null_float);
            System.out.println("ACT value: " + type_null_float);
            System.out.println("valueOf():" + Double.valueOf("-2155882919525625344.000000000000"));
            System.out.flush();
            return error("type_null_float", type_null_float, valid.type_null_float);
        }

        // col 22
        Double type_not_null_float = Double.valueOf(row[++col]);
        if (!type_not_null_float.equals(valid.type_not_null_float))
            return error("type_not_null_float", type_not_null_float, valid.type_not_null_float);

        // col 23
        String type_null_varchar25 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar25 == valid.type_null_varchar25 ||
              type_null_varchar25.equals(valid.type_null_varchar25)))
            return error("type_null_varchar25", type_null_varchar25, valid.type_null_varchar25);

        // col 24
        String type_not_null_varchar25 = row[++col];
        if (!type_not_null_varchar25.equals(valid.type_not_null_varchar25))
            return error("type_not_null_varchar25", type_not_null_varchar25, valid.type_not_null_varchar25);

        // col 25
        String type_null_varchar128 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar128 == valid.type_null_varchar128 ||
              type_null_varchar128.equals(valid.type_null_varchar128)))
            return error("type_null_varchar128", type_null_varchar128, valid.type_null_varchar128);

        // col 26
        String type_not_null_varchar128 = row[++col];
        if (!type_not_null_varchar128.equals(valid.type_not_null_varchar128))
            return error("type_not_null_varchar128", type_not_null_varchar128, valid.type_not_null_varchar128);

        // col 27
        String type_null_varchar1024 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar1024 == valid.type_null_varchar1024 ||
              type_null_varchar1024.equals(valid.type_null_varchar1024)))
            return error("type_null_varchar1024", type_null_varchar1024, valid.type_null_varchar1024);

        // col 28
        String type_not_null_varchar1024 = row[++col];
        if (!type_not_null_varchar1024.equals(valid.type_not_null_varchar1024))
            return error("type_not_null_varchar1024", type_not_null_varchar1024, valid.type_not_null_varchar1024);

        return null;
    }

    private ValidationErr error(String msg, Object val, Object exp) throws ValidationErr {
        System.err.println("ERROR: " + msg + " " + val + " " + exp);
        return new ValidationErr(msg, val, exp);
    }

    int m_partitions = 0;
    HashMap<Integer, TreeMap<Long,Long>> m_rowTxnIds = new HashMap<>();

    String m_topic = null;
    String m_zookeeper = null;

    static {
        VoltDB.setDefaultTimezone();
    }

    public void stopConsumer() {
        for (RemoteHost rh : m_hosts) {
            rh.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        final ExportKafkaOnServerVerifier verifier = new ExportKafkaOnServerVerifier();
        try
        {
            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            System.out.println("Shuttind Down...");
                            verifier.stopConsumer();
                        }
                    });

            boolean skinny = verifier.verifySetup(args);

            if (skinny) {
                verifier.verifySkinny();
            } else {
                verifier.verifyFat();
            }

        }
        catch(IOException e) {
            e.printStackTrace(System.err);
            System.exit(-1);
        }
        catch (ValidationErr e ) {
            System.err.println("Validation error: " + e.toString());
            System.exit(-1);
        }
        System.exit(0);
    }

    public static class RoughCSVTokenizer {

        private RoughCSVTokenizer() {
        }

        private static void moveToBuffer(List<String> resultBuffer, StringBuilder buf) {
            resultBuffer.add(buf.toString());
            buf.delete(0, buf.length());
        }

        public static String[] tokenize(String csv) {
            List<String> resultBuffer = new java.util.ArrayList<String>();

            if (csv != null) {
                int z = csv.length();
                Character openingQuote = null;
                boolean trimSpace = false;
                StringBuilder buf = new StringBuilder();

                for (int i = 0; i < z; ++i) {
                    char c = csv.charAt(i);
                    trimSpace = trimSpace && Character.isWhitespace(c);
                    if (c == '"' || c == '\'') {
                        if (openingQuote == null) {
                            openingQuote = c;
                            int bi = 0;
                            while (bi < buf.length()) {
                                if (Character.isWhitespace(buf.charAt(bi))) {
                                    buf.deleteCharAt(bi);
                                } else {
                                    bi++;
                                }
                            }
                        }
                        else if (openingQuote == c ) {
                            openingQuote = null;
                            trimSpace = true;
                        }
                    }
                    else if (c == '\\') {
                        if ((z > i + 1)
                            && ((csv.charAt(i + 1) == '"')
                                || (csv.charAt(i + 1) == '\\'))) {
                            buf.append(csv.charAt(i + 1));
                            ++i;
                        } else {
                            buf.append("\\");
                        }
                    } else {
                        if (openingQuote != null) {
                            buf.append(c);
                        } else {
                            if (c == ',') {
                                moveToBuffer(resultBuffer, buf);
                            } else {
                                if (!trimSpace) buf.append(c);
                            }
                        }
                    }
                }
                moveToBuffer(resultBuffer, buf);
            }

            String[] result = new String[resultBuffer.size()];
            return resultBuffer.toArray(result);
        }
    }
}
