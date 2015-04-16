/* Copyright (c) 2001-2011, The HSQL Development Group
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

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;

/**
 * Handles the differences between JDK 5 and above
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
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

    public static IOException toIOException(Throwable t) {

        if (t instanceof IOException) {
            return (IOException) t;
        }

//#ifdef JAVA6
        return new IOException(t);

//#else
/*
        IOException e = new IOException(t.toString());
        try {
            e.initCause(t);
        } catch (Throwable e1) {}

        return e;

*/

//#endif JAVA6
    }

    static final BigDecimal BD_1  = BigDecimal.valueOf(1L);
    static final BigDecimal MBD_1 = BigDecimal.valueOf(-1L);

    public static int precision(BigDecimal o) {

        if (o == null) {
            return 0;
        }

//#ifdef JAVA6
        int precision;

        if (o.compareTo(BD_1) < 0 && o.compareTo(MBD_1) > 0) {
            precision = o.scale();
        } else {
            precision = o.precision();
        }

        return precision;

//#else
/*
        if (o.compareTo(BD_1) < 0 && o.compareTo(MBD_1) > 0) {
            return o.scale();
        }

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

    public static void setLogToSystem(boolean value) {

        try {
            PrintWriter newPrintWriter = (value) ? new PrintWriter(System.out)
                                                 : null;

            DriverManager.setLogWriter(newPrintWriter);
        } catch (Exception e) {}
    }
}
