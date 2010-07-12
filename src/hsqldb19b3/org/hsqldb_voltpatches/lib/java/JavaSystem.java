/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib.java;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.util.Properties;
import java.text.Collator;
import java.io.RandomAccessFile;

/**
 * Handles the differences between JDK 1.1.x and 1.2.x and above
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.8.0
 */
public class JavaSystem {

    // variables to track rough count on object creation, to use in gc
    public static int gcFrequency;
    public static int memoryRecords;

    // Garbage Collection
    public static void gc() {

        if ((gcFrequency > 0) && (memoryRecords > gcFrequency)) {
            memoryRecords = 0;

            System.gc();
        }
    }

    public static int precision(BigDecimal o) {

        if (o == null) {
            return 0;
        }

//#ifdef JAVA6
        return o.precision();

//#else
/*
        BigInteger big  = o.unscaledValue();
        int        sign = big.signum() == -1 ? 1
                                             : 0;

        return big.toString().length() - sign;
*/

//#endif JAVA6
    }

    public static String toString(BigDecimal o) {

        if (o == null) {
            return null;
        }

//#ifdef JAVA6
        return o.toPlainString();

//#else
/*
        return o.toString();
*/

//#endif JAVA6
    }

    public static int CompareIngnoreCase(String a, String b) {

//#ifdef JAVA2FULL
        return a.compareToIgnoreCase(b);

//#else
/*
        return a.toUpperCase().compareTo(b.toUpperCase());
*/

//#endif JAVA2
    }

    public static double parseDouble(String s) {

//#ifdef JAVA2FULL
        return Double.parseDouble(s);

//#else
/*
        return new Double(s).doubleValue();
*/

//#endif JAVA2
    }

    public static BigInteger unscaledValue(BigDecimal o) {

//#ifdef JAVA2FULL
        return o.unscaledValue();

//#else
/*
        int scale = o.scale();
        return o.movePointRight(scale).toBigInteger();
*/

//#endif
    }

    public static void setLogToSystem(boolean value) {

//#ifdef JAVA2FULL
        try {
            PrintWriter newPrintWriter = (value) ? new PrintWriter(System.out)
                                                 : null;

            DriverManager.setLogWriter(newPrintWriter);
        } catch (Exception e) {}

//#else
/*
        try {
            PrintStream newOutStream = (value) ? System.out
                                               : null;
            DriverManager.setLogStream(newOutStream);
        } catch (Exception e){}
*/

//#endif
    }

    public static void deleteOnExit(File f) {

//#ifdef JAVA2FULL
        f.deleteOnExit();

//#endif
    }

    public static void saveProperties(Properties props, String name,
                                      OutputStream os) throws IOException {

//#ifdef JAVA2FULL
        props.store(os, name);

//#else
/*
        props.save(os, name);
*/

//#endif
    }

    public static void runFinalizers() {

//#ifdef JAVA2FULL
        System.runFinalizersOnExit(true);

//#endif
    }

    public static boolean createNewFile(File file) {

//#ifdef JAVA2FULL
        try {
            return file.createNewFile();
        } catch (IOException e) {}

        return false;

//#else
/*
        return true;
*/

//#endif
    }

    public static void setRAFileLength(RandomAccessFile raFile,
                                       long length) throws IOException {

//#ifdef JAVA2FULL
        raFile.setLength(length);

//#endif
    }
}
