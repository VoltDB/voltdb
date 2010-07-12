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


package org.hsqldb_voltpatches.persist;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Wrapper for java.util.Properties to limit values to Specific types and
 * allow saving and loading.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class HsqlProperties {

    // column number mappings
    public static final int indexName         = 0;
    public static final int indexType         = 1;
    public static final int indexClass        = 2;
    public static final int indexIsRange      = 3;
    public static final int indexDefaultValue = 4;
    public static final int indexRangeLow     = 5;
    public static final int indexRangeHigh    = 6;
    public static final int indexValues       = 7;
    public static final int indexLimit        = 9;

    //
    public static final int NO_VALUE_FOR_KEY = 1;
    protected String        fileName;
    protected Properties    stringProps;
    protected int[]         errorCodes = new int[0];
    protected String[]      errorKeys  = new String[0];
    protected boolean       resource   = false;
    protected FileAccess    fa;

    public HsqlProperties() {
        stringProps = new Properties();
        fileName    = null;
    }

    public HsqlProperties(String fileName) {

        stringProps   = new Properties();
        this.fileName = fileName;
        fa            = FileUtil.getDefaultInstance();
    }

    public HsqlProperties(String fileName, FileAccess accessor, boolean b) {

        stringProps   = new Properties();
        this.fileName = fileName;
        resource      = b;
        fa            = accessor;
    }

    public HsqlProperties(Properties props) {
        stringProps = props;
    }

    public void setFileName(String name) {
        fileName = name;
    }

    public String setProperty(String key, int value) {
        return setProperty(key, Integer.toString(value));
    }

    public String setProperty(String key, boolean value) {
        return setProperty(key, String.valueOf(value));
    }

    public String setProperty(String key, String value) {
        return (String) stringProps.put(key, value);
    }

    public String setPropertyIfNotExists(String key, String value) {

        value = getProperty(key, value);

        return setProperty(key, value);
    }

    public Properties getProperties() {
        return stringProps;
    }

    public String getProperty(String key) {
        return stringProps.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return stringProps.getProperty(key, defaultValue);
    }

    public int getIntegerProperty(String key, int defaultValue) {

        String prop = getProperty(key);

        try {
            if (prop != null) {
                defaultValue = Integer.parseInt(prop);
            }
        } catch (NumberFormatException e) {}

        return defaultValue;
    }

    public int getIntegerProperty(String key, int defaultValue, int minimum,
                                  int maximum) {

        String  prop     = getProperty(key);
        boolean badvalue = false;

        try {
            defaultValue = Integer.parseInt(prop);
        } catch (NumberFormatException e) {}

        if (defaultValue < minimum) {
            defaultValue = minimum;
            badvalue     = true;
        } else if (defaultValue > maximum) {
            defaultValue = maximum;
            badvalue     = true;
        }

        return defaultValue;
    }

    /**
     * Choice limited to values list, defaultValue must be in the values list.
     */
    public int getIntegerProperty(String key, int defaultValue, int[] values) {

        String prop  = getProperty(key);
        int    value = defaultValue;

        try {
            if (prop != null) {
                value = Integer.parseInt(prop);
            }
        } catch (NumberFormatException e) {}

        if (ArrayUtil.find(values, value) == -1) {
            return defaultValue;
        }

        return value;
    }

    public boolean isPropertyTrue(String key) {
        return isPropertyTrue(key, false);
    }

    public boolean isPropertyTrue(String key, boolean defaultValue) {

        String value = stringProps.getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        return value.toLowerCase().equals("true");
    }

    public void removeProperty(String key) {
        stringProps.remove(key);
    }

    public void addProperties(Properties props) {

        if (props == null) {
            return;
        }

        Enumeration keys = props.keys();

        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();

            this.stringProps.put(key, props.get(key));
        }
    }

    public void addProperties(HsqlProperties props) {

        if (props == null) {
            return;
        }

        addProperties(props.stringProps);
    }

// oj@openoffice.org
    public boolean checkFileExists() throws IOException {

        String propFilename = fileName + ".properties";

        return fa.isStreamElement(propFilename);
    }

    public boolean load() throws Exception {

        if (!checkFileExists()) {
            return false;
        }

        if (fileName == null || fileName.length() == 0) {
            throw new FileNotFoundException(
                Error.getMessage(ErrorCode.M_HsqlProperties_load));
        }

        InputStream fis           = null;
        String      propsFilename = fileName + ".properties";

// oj@openoffice.org
        try {
            fis = resource ? getClass().getResourceAsStream(propsFilename)
                           : fa.openInputStreamElement(propsFilename);

            stringProps.load(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }

        return true;
    }

    /**
     *  Saves the properties.
     */
    public void save() throws Exception {

        if (fileName == null || fileName.length() == 0) {
            throw new java.io.FileNotFoundException(
                Error.getMessage(ErrorCode.M_HsqlProperties_load));
        }

        String filestring = fileName + ".properties";

        save(filestring);
    }

    /**
     *  Saves the properties using JDK2 method if present, otherwise JDK1.
     */
    public void save(String fileString) throws Exception {

// oj@openoffice.org
        fa.createParentDirs(fileString);

        OutputStream        fos = fa.openOutputStreamElement(fileString);
        FileAccess.FileSync outDescriptor = fa.getFileSync(fos);

        JavaSystem.saveProperties(
            stringProps,
            HsqlDatabaseProperties.PRODUCT_NAME + " "
            + HsqlDatabaseProperties.THIS_FULL_VERSION, fos);
        fos.flush();
        outDescriptor.sync();
        fos.close();

        return;
    }

    /**
     * Adds the error code and the key to the list of errors. This list
     * is populated during construction or addition of elements and is used
     * outside this class to act upon the errors.
     */
    private void addError(int code, String key) {

        errorCodes = (int[]) ArrayUtil.resizeArray(errorCodes,
                errorCodes.length + 1);
        errorKeys = (String[]) ArrayUtil.resizeArray(errorKeys,
                errorKeys.length + 1);
        errorCodes[errorCodes.length - 1] = code;
        errorKeys[errorKeys.length - 1]   = key;
    }

    /**
     * Creates and populates an HsqlProperties Object from the arguments
     * array of a Main method. Properties are in the form of "-key value"
     * pairs. Each key is prefixed with the type argument and a dot before
     * being inserted into the properties Object. <p>
     *
     * "--help" is treated as a key with no value and not inserted.
     */
    public static HsqlProperties argArrayToProps(String[] arg, String type) {

        HsqlProperties props = new HsqlProperties();

        for (int i = 0; i < arg.length; i++) {
            String p = arg[i];

            if (p.equals("--help") || p.equals("-help")) {
                props.addError(NO_VALUE_FOR_KEY, p.substring(1));
            } else if (p.startsWith("--")) {
                String value = i + 1 < arg.length ? arg[i + 1]
                                                  : "";

                props.setProperty(type + "." + p.substring(2), value);

                i++;
            } else if (p.charAt(0) == '-') {
                String value = i + 1 < arg.length ? arg[i + 1]
                                                  : "";

                props.setProperty(type + "." + p.substring(1), value);

                i++;
            }
        }

        return props;
    }

    /**
     * Creates and populates a new HsqlProperties Object using a string
     * such as "key1=value1;key2=value2". <p>
     *
     * The string that represents the = sign above is specified as pairsep
     * and the one that represents the semicolon is specified as delimiter,
     * allowing any string to be used for either.<p>
     *
     * Leading / trailing spaces around the keys and values are discarded.<p>
     *
     * The string is parsed by (1) subdividing into segments by delimiter
     * (2) subdividing each segment in two by finding the first instance of
     * the pairsep (3) trimming each pair of segments from step 2 and
     * inserting into the properties object.<p>
     *
     * Each key is prefixed with the type argument and a dot before being
     * inserted.<p>
     *
     * Any key without a value is added to the list of errors.
     */
    public static HsqlProperties delimitedArgPairsToProps(String s,
            String pairsep, String dlimiter, String type) {

        HsqlProperties props       = new HsqlProperties();
        int            currentpair = 0;

        while (true) {
            int nextpair = s.indexOf(dlimiter, currentpair);

            if (nextpair == -1) {
                nextpair = s.length();
            }

            // find value within the segment
            int valindex = s.substring(0, nextpair).indexOf(pairsep,
                                       currentpair);

            if (valindex == -1) {
                props.addError(NO_VALUE_FOR_KEY,
                               s.substring(currentpair, nextpair).trim());
            } else {
                String key = s.substring(currentpair, valindex).trim();
                String value = s.substring(valindex + pairsep.length(),
                                           nextpair).trim();

                if (type != null) {
                    key = type + "." + key;
                }

                props.setProperty(key, value);
            }

            if (nextpair == s.length()) {
                break;
            }

            currentpair = nextpair + dlimiter.length();
        }

        return props;
    }

    public Enumeration propertyNames() {
        return stringProps.propertyNames();
    }

    public boolean isEmpty() {
        return stringProps.isEmpty();
    }

    public String[] getErrorKeys() {
        return errorKeys;
    }

    public static Object[] getMeta(String name, int type,
                                   String defaultValue) {

        Object[] row = new Object[indexLimit];

        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "java.lang.String";
        row[indexDefaultValue] = defaultValue;

        return row;
    }

    public static Object[] getMeta(String name, int type,
                                   boolean defaultValue) {

        Object[] row = new Object[indexLimit];

        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "boolean";
        row[indexDefaultValue] = defaultValue ? Boolean.TRUE
                                              : Boolean.FALSE;

        return row;
    }

    public static Object[] getMeta(String name, int type, int defaultValue,
                                   int[] values) {

        Object[] row = new Object[indexLimit];

        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "int";
        row[indexDefaultValue] = ValuePool.getInt(defaultValue);
        row[indexValues]       = values;

        return row;
    }

    public static Object[] getMeta(String name, int type, int defaultValue,
                                   int rangeLow, int rangeHigh) {

        Object[] row = new Object[indexLimit];

        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "int";
        row[indexDefaultValue] = ValuePool.getInt(defaultValue);
        row[indexIsRange]      = Boolean.TRUE;
        row[indexRangeLow]     = ValuePool.getInt(rangeLow);
        row[indexRangeHigh]    = ValuePool.getInt(rangeHigh);

        return row;
    }

    public String toString() {

        StringBuffer sb;

        sb = new StringBuffer();

        sb.append('[');

        int         len = stringProps.size();
        Enumeration en  = stringProps.propertyNames();

        for (int i = 0; i < len; i++) {
            String key = (String) en.nextElement();

            sb.append(key);
            sb.append('=');
            sb.append(stringProps.get(key));

            if (i + 1 < len) {
                sb.append(',');
                sb.append(' ');
            }

            sb.append(']');
        }

        return sb.toString();
    }
}
