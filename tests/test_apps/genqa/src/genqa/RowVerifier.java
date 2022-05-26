/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.spearce_voltpatches.jgit.transport.OpenSshConfig;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.types.TimestampType;
import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Throwables;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class RowVerifier {

    // force client and server to have the same timezone so validation of
    // datetime fields is accureate
    static {
        VoltDB.setDefaultTimezone();
    }



    static VoltLogger log = new VoltLogger("RowVerifier");

    public static ValidationErr verifyRow(String[] row) throws ValidationErr
    {
        if (row.length < 29)
        {
            log.info("ERROR: Unexpected number of columns for the following row:\n\t Expected 29, Found: "
                    + row.length + "Row:" + Arrays.toString(row));
            return new ValidationErr("number of columns", row.length, 29);
        }

        int col = 5; // col offset is always pre-incremented.
        Long txnid = Long.parseLong(row[++col]); // col 6
        Long rowid = Long.parseLong(row[++col]); // col 7
        // matches VoltProcedure.getSeededRandomNumberGenerator()
        Random prng = new Random(txnid);
        SampleRecord valid = new SampleRecord(rowid, prng);

        //
        //  Uncomment these lines to dump the column data if needed for debugging.
        //
        // System.out.println("RabbitMQ row:\n\t " + Arrays.toString(row));
        // System.out.println("Generated row:\n\t " + valid.toString());

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
            log.info("CSV value: " + row[col]);
            log.info("EXP value: " + valid.type_null_timestamp.toString());
            log.info("ACT value: " + type_null_timestamp.toString());
            return error("type_null_timestamp", type_null_timestamp, valid.type_null_timestamp);
        }

        // col 18
        // skip, should be current time, not random so don't count on a match
        // TimestampType type_not_null_timestamp = new TimestampType(row[++col]);
        // if (!type_not_null_timestamp.equals(valid.type_not_null_timestamp))
        //     return error("type_null_timestamp", type_not_null_timestamp, valid.type_not_null_timestamp);
        col++; // make sure to move onto the next column...

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
            log.info("CSV value: " + row[col]);
            log.info("EXP value: " + valid.type_null_float);
            log.info("ACT value: " + type_null_float);
            log.info("valueOf():" + Double.valueOf("-2155882919525625344.000000000000"));
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

    public static ValidationErr error(String msg, Object val, Object exp) throws ValidationErr {
        log.error("ERROR: " + msg + "Value:" + val + "\nExpected:" + exp);
        return new ValidationErr(msg, val, exp);
    }

}
