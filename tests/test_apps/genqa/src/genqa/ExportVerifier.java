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
package genqa;

import genqa.procedures.SampleRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;

import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class ExportVerifier {

    public static long FILE_TIMEOUT_MS = 5 * 60 * 1000; // 5 mins

    public static long VALIDATION_REPORT_INTERVAL = 100000;

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

        @Override
        public String toString() {
            return msg + " Value: " + value + " Expected: " + expected;
        }
    }

    ExportVerifier()
    {
    }

    void verify(String[] args) throws IOException, ValidationErr
    {
        m_partitions = Integer.parseInt(args[0]);

        for (int i = 0; i < m_partitions; i++)
        {
            m_rowTxnIds.put(i, new HashSet<Long>());
        }

        String[] row;
        long ttlVerified = 0;
        m_dataPath = new File(args[1]);
        if (!m_dataPath.exists() || !m_dataPath.isDirectory())
        {
            throw new IOException("Issue with export data path");
        }

        //checkForMoreExportFiles();
        CSVReader csv = openNextExportFile();

        m_clientPath = new File(args[2]);
        if (!m_clientPath.exists() || !m_clientPath.isDirectory())
        {
            if (!m_clientPath.mkdir()) {
                throw new IOException("Issue with transaction ID path");
            }
        }

        //checkForMoreClientFiles();
        BufferedReader txnIdReader = openNextClientFile();

        boolean quit = false;
        boolean more_rows = true;
        boolean more_txnids = true;
        while (!quit)
        {
            more_rows = true;
            while (!canCheckClient() && more_rows)
            {
                row = csv.readNext();
                if (row == null)
                {
                    csv = openNextExportFile();
                    if (csv == null)
                    {
                        System.out.println("No more export rows");
                        more_rows = false;
                        break;
                    }
                    else
                    {
                        row = csv.readNext();
                    }
                }

                verifyRow(row);
                if (++ttlVerified % VALIDATION_REPORT_INTERVAL == 0) {
                    System.out.println("Verified " + ttlVerified + " rows.");
                }
                Integer partition = Integer.parseInt(row[3]);
                Long rowTxnId = Long.parseLong(row[6]);
                boolean goodness = m_rowTxnIds.get(partition).add(rowTxnId);
                if (!goodness)
                {
                    System.out.println("Duplicate TXN ID in export stream: " + rowTxnId);
                    System.exit(-1);
                }
                else
                {
                    //System.out.println("Added txnId: " + rowTxnId + " to outstanding export");
                }
            }

            // If we've pulled in rows for every partition, or there are
            // no more export rows, and there are still unchecked client txnids,
            // attempt to validate as many client txnids as possible
            more_txnids = true;
            while ((canCheckClient() || !more_rows) && more_txnids)
            {
                String txnId = txnIdReader.readLine();
                if (txnId == null)
                {
                    txnIdReader = openNextClientFile();
                    if (txnIdReader == null)
                    {
                        System.out.println("No more client txn IDs");
                        more_txnids = false;
                    }
                    else
                    {
                        txnId = txnIdReader.readLine();
                    }
                }
                if (txnId != null)
                {
                    //System.out.println("Added txnId: " + txnId + " to outstanding client");
                    m_clientTxnIds.add(Long.parseLong(txnId));
                }
                boolean progress = true;
                while (!m_clientTxnIds.isEmpty() && progress)
                {
                    Long txnid = m_clientTxnIds.first();
                    if (foundTxnId(txnid))
                    {
                        m_clientTxnIds.pollFirst();
                    }
                    else
                    {
                        progress = false;
                    }
                }
            }

            if (!more_rows || !more_txnids)
            {
                quit = true;
            }
        }
        if (more_rows || more_txnids)
        {
            System.out.println("Something wasn't drained");
            System.out.println("client txns remaining: " + m_clientTxnIds.size());
            System.out.println("Export rows remaining: ");
            int total = 0;
            for (int i = 0; i < m_partitions; i++)
            {
                total += m_rowTxnIds.get(i).size();
                System.out.println("\tpartition: " + i + ", size: " + m_rowTxnIds.get(i).size());
            }
            if (total != 0 && m_clientTxnIds.size() != 0)
            {
                System.out.println("THIS IS A REAL ERROR?!");
            }
        }
    }

    private File[] checkForMoreFiles(File path, File[] files, FileFilter acceptor,
                                   Comparator<File> comparator) throws ValidationErr
    {
        int old_length = files.length;
        long start_time = System.currentTimeMillis();
        while (files.length == old_length || files.length == 0)
        {
            files = path.listFiles(acceptor);
            long now = System.currentTimeMillis();
            if ((now - start_time) > FILE_TIMEOUT_MS)
            {
                throw new ValidationErr("Timed out waiting on new files in " + path.getName()+ ".\n" +
                                        "This indicates a mismatch in the transaction streams between the client logs and the export data or the death of something important.",
                                        null, null);
            }
        }
        Arrays.sort(files, comparator);
        return files;
    }

    private void checkForMoreExportFiles() throws ValidationErr
    {
        FileFilter acceptor = new FileFilter()
        {
            public boolean accept(File pathname) {
                return !pathname.getName().contains("active");
            }
        };

        Comparator<File> comparator = new Comparator<File>()
        {
            public int compare(File f1, File f2)
            {
                long first_ts = Long.parseLong((f1.getName().split("-")[3]).split("\\.")[0]);
                long second_ts = Long.parseLong((f2.getName().split("-")[3]).split("\\.")[0]);
                if (first_ts != second_ts)
                {
                    return (int)(first_ts - second_ts);
                }
                else
                {
                    long first_txnid = Long.parseLong(f1.getName().split("-")[1]);
                    long second_txnid = Long.parseLong(f2.getName().split("-")[1]);
                    if (first_txnid < second_txnid)
                    {
                        return -1;
                    }
                    else if (first_txnid > second_txnid)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
            }
        };

        m_exportFiles = checkForMoreFiles(m_dataPath, m_exportFiles, acceptor, comparator);
        for (int i = 0; i < m_exportFiles.length; i++)
        {
            System.out.println("" + i + ": " + m_exportFiles[i].getName());
        }
    }

    private void checkForMoreClientFiles() throws ValidationErr
    {
        FileFilter acceptor = new FileFilter()
        {
            public boolean accept(File pathname) {
                return pathname.getName().contains("dude");
            }
        };

        Comparator<File> comparator = new Comparator<File>()
        {
            public int compare(File f1, File f2)
            {
                long first = Long.parseLong(f1.getName().split("-")[0]);
                long second = Long.parseLong(f2.getName().split("-")[0]);
                return (int)(first - second);
            }
        };

        m_clientFiles = checkForMoreFiles(m_clientPath, m_clientFiles, acceptor, comparator);
    }

    private CSVReader openNextExportFile() throws FileNotFoundException, ValidationErr
    {
        CSVReader exportreader = null;
        if (m_exportIndex == m_exportFiles.length)
        {
            for (int i = 0; i < m_exportIndex; i++)
            {
                m_exportFiles[i].delete();
            }
            checkForMoreExportFiles();
            m_exportIndex = 0;
        }
        File data = m_exportFiles[m_exportIndex];
        System.out.println("Opening export file: " + data.getName());
        FileInputStream dataIs = new FileInputStream(data);
        Reader reader = new InputStreamReader(dataIs);
        exportreader = new CSVReader(reader);
        m_exportIndex++;
        return exportreader;
    }

    private BufferedReader openNextClientFile() throws FileNotFoundException, ValidationErr
    {
        BufferedReader clientreader = null;
        if (m_clientIndex == m_clientFiles.length)
        {
            for (int i = 0; i < m_clientIndex; i++)
            {
                m_clientFiles[i].delete();
            }
            checkForMoreClientFiles();
            m_clientIndex = 0;
        }
        File data = m_clientFiles[m_clientIndex];
        System.out.println("Opening client file: " + data.getName());
        FileInputStream dataIs = new FileInputStream(data);
        Reader reader = new InputStreamReader(dataIs);
        clientreader = new BufferedReader(reader);
        m_clientIndex++;
        return clientreader;
    }

    private boolean canCheckClient()
    {
        boolean can_check = true;
        for (int i = 0; i < m_partitions; i++)
        {
            if (m_rowTxnIds.get(i).isEmpty())
            {
                can_check = false;
                break;
            }
        }
        return can_check;
    }

    private boolean foundTxnId(Long txnId)
    {
        boolean found = false;
        for (int i = 0; i < m_partitions; i++)
        {
            if (m_rowTxnIds.get(i).contains(txnId))
            {
                //System.out.println("Found txnId: " + txnId + " in partition: " + i);
                m_rowTxnIds.get(i).remove(txnId);
                found = true;
                break;
            }
        }
        return found;
    }

    private void verifyRow(String[] row) throws ValidationErr {
        int col = 5; // col offset is always pre-incremented.
        Long txnid = Long.parseLong(row[++col]);
        Long rowid = Long.parseLong(row[++col]);

        // matches VoltProcedure.getSeededRandomNumberGenerator()
        Random prng = new Random(txnid);
        SampleRecord valid = new SampleRecord(rowid, prng);

        Byte rowid_group = Byte.parseByte(row[++col]);
        if (rowid_group != valid.rowid_group)
            error("rowid_group invalid", rowid_group, valid.rowid_group);

        Byte type_null_tinyint = row[++col].equals("NULL") ? null : Byte.valueOf(row[col]);
        if ( (!(type_null_tinyint == null && valid.type_null_tinyint == null)) &&
             (!type_null_tinyint.equals(valid.type_null_tinyint)) )
            error("type_not_null_tinyint", type_null_tinyint, valid.type_null_tinyint);

        Byte type_not_null_tinyint = Byte.valueOf(row[++col]);
        if (!type_not_null_tinyint.equals(valid.type_not_null_tinyint))
            error("type_not_null_tinyint", type_not_null_tinyint, valid.type_not_null_tinyint);

        Short type_null_smallint = row[++col].equals("NULL") ? null : Short.valueOf(row[col]);
        if ( (!(type_null_smallint == null && valid.type_null_smallint == null)) &&
             (!type_null_smallint.equals(valid.type_null_smallint)) )
            error("type_null_smallint", type_null_smallint, valid.type_null_smallint);

        Short type_not_null_smallint = Short.valueOf(row[++col]);
        if (!type_not_null_smallint.equals(valid.type_not_null_smallint))
            error("type_null_smallint", type_not_null_smallint, valid.type_not_null_smallint);

        Integer type_null_integer = row[++col].equals("NULL") ? null : Integer.valueOf(row[col]);
        if ( (!(type_null_integer == null && valid.type_null_integer == null)) &&
             (!type_null_integer.equals(valid.type_null_integer)) )
            error("type_null_integer", type_null_integer, valid.type_null_integer);

        Integer type_not_null_integer = Integer.valueOf(row[++col]);
        if (!type_not_null_integer.equals(valid.type_not_null_integer))
            error("type_not_null_integer", type_not_null_integer, valid.type_not_null_integer);

        Long type_null_bigint = row[++col].equals("NULL") ? null : Long.valueOf(row[col]);
        if ( (!(type_null_bigint == null && valid.type_null_bigint == null)) &&
             (!type_null_bigint.equals(valid.type_null_bigint)) )
            error("type_null_bigint", type_null_bigint, valid.type_null_bigint);

        Long type_not_null_bigint = Long.valueOf(row[++col]);
        if (!type_not_null_bigint.equals(valid.type_not_null_bigint))
            error("type_not_null_bigint", type_not_null_bigint, valid.type_not_null_bigint);

        // The ExportToFileClient truncates microseconds. Construct a TimestampType here
        // that also truncates microseconds.
        TimestampType type_null_timestamp;
        if (row[++col].equals("NULL")) {
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
            error("type_null_timestamp", type_null_timestamp, valid.type_null_timestamp);
        }

        TimestampType type_not_null_timestamp = new TimestampType(row[++col]);
        if (!type_not_null_timestamp.equals(valid.type_not_null_timestamp))
            error("type_null_timestamp", type_not_null_timestamp, valid.type_not_null_timestamp);

        BigDecimal type_null_decimal = row[++col].equals("NULL") ? null : new BigDecimal(row[col]);
        if ( (!(type_null_decimal == null && valid.type_null_decimal == null)) &&
             (!type_null_decimal.equals(valid.type_null_decimal)) )
            error("type_null_decimal", type_null_decimal, valid.type_null_decimal);

        BigDecimal type_not_null_decimal = new BigDecimal(row[++col]);
        if (!type_not_null_decimal.equals(valid.type_not_null_decimal))
            error("type_not_null_decimal", type_not_null_decimal, valid.type_not_null_decimal);

        Double type_null_float = row[++col].equals("NULL") ? null : Double.valueOf(row[col]);
        if ( (!(type_null_float == null && valid.type_null_float == null)) &&
             (!type_null_float.equals(valid.type_null_float)) )
        {
            System.out.println("CSV value: " + row[col]);
            System.out.println("EXP value: " + valid.type_null_float);
            System.out.println("ACT value: " + type_null_float);
            System.out.println("valueOf():" + Double.valueOf("-2155882919525625344.000000000000"));
            System.out.flush();
            error("type_null_float", type_null_float, valid.type_null_float);
        }

        Double type_not_null_float = Double.valueOf(row[++col]);
        if (!type_not_null_float.equals(valid.type_not_null_float))
            error("type_not_null_float", type_not_null_float, valid.type_not_null_float);

        String type_null_varchar25 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar25 == valid.type_null_varchar25 ||
              type_null_varchar25.equals(valid.type_null_varchar25)))
            error("type_null_varchar25", type_null_varchar25, valid.type_null_varchar25);

        String type_not_null_varchar25 = row[++col];
        if (!type_not_null_varchar25.equals(valid.type_not_null_varchar25))
            error("type_not_null_varchar25", type_not_null_varchar25, valid.type_not_null_varchar25);

        String type_null_varchar128 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar128 == valid.type_null_varchar128 ||
              type_null_varchar128.equals(valid.type_null_varchar128)))
            error("type_null_varchar128", type_null_varchar128, valid.type_null_varchar128);

        String type_not_null_varchar128 = row[++col];
        if (!type_not_null_varchar128.equals(valid.type_not_null_varchar128))
            error("type_not_null_varchar128", type_not_null_varchar128, valid.type_not_null_varchar128);

        String type_null_varchar1024 = row[++col].equals("NULL") ? null : row[col];
        if (!(type_null_varchar1024 == valid.type_null_varchar1024 ||
              type_null_varchar1024.equals(valid.type_null_varchar1024)))
            error("type_null_varchar1024", type_null_varchar1024, valid.type_null_varchar1024);

        String type_not_null_varchar1024 = row[++col];
        if (!type_not_null_varchar1024.equals(valid.type_not_null_varchar1024))
            error("type_not_null_varchar1024", type_not_null_varchar1024, valid.type_not_null_varchar1024);
    }

    private void error(String msg, Object val, Object exp) throws ValidationErr {
        System.err.println("ERROR: " + msg + " " + val + " " + exp);
        throw new ValidationErr(msg, val, exp);
    }

    int m_partitions = 0;
    HashMap<Integer, HashSet<Long>> m_rowTxnIds =
        new HashMap<Integer, HashSet<Long>>();

    TreeSet<Long> m_clientTxnIds = new TreeSet<Long>();
    File m_dataPath = null;
    File[] m_exportFiles = {};
    int m_exportIndex = 0;
    File m_clientPath = null;
    File[] m_clientFiles = {};
    int m_clientIndex = 0;

    public static void main(String[] args) {
        ExportVerifier verifier = new ExportVerifier();
        try
        {
            verifier.verify(args);
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


}
