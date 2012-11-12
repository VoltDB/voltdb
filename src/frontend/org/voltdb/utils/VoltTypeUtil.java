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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.TimestampType;

public abstract class VoltTypeUtil {
    private static final Logger LOG = Logger.getLogger(VoltTypeUtil.class.getName());

    protected final static Random rand = new Random();
    protected static final Long DATE_STOP = System.currentTimeMillis() / 1000;
    protected static final Long DATE_START = VoltTypeUtil.DATE_STOP - 153792000;

    public static Object getRandomValue(VoltType type) {
        return getRandomValue(type, 32, 0, rand);
    }

    public static Object getRandomValue(VoltType type, int maxSize) {
        return getRandomValue(type, maxSize, 0, rand);
    }

    public static Object getRandomValue(VoltType type, int maxSize, double nullFraction, Random r) {
        assert(type != null);
        assert(maxSize > 0);
        assert(nullFraction >= 0.0);
        assert(nullFraction <= 1.0);
        assert(r != null);

        // return null for some fraction of requests
        if (r.nextDouble() < nullFraction) {
            return null;
        }

        Object ret = null;
        switch (type) {
            // --------------------------------
            // INTEGERS
            // --------------------------------
            case TINYINT:
                ret = Byte.valueOf((byte) Math.abs(r.nextInt() % 128));
                break;
            case SMALLINT:
                ret = Short.valueOf((short) Math.abs(r.nextInt() % 32768));
                break;
            case INTEGER:
                ret = Integer.valueOf(Math.abs(r.nextInt() % 100000));
                break;
            case BIGINT:
                ret = Long.valueOf(Math.abs(r.nextInt() % 100000));
                break;
            // --------------------------------
            // FLOATS
            // --------------------------------
            case FLOAT:
                ret = Double.valueOf(Math.abs(r.nextDouble()));
                break;
            // --------------------------------
            // STRINGS
            // --------------------------------
            case STRING: {
                int size = r.nextInt(maxSize) + 1;
                String ret_str = "";
                for (int ctr = 0; ctr < size; ctr++) {
                    char data = (char)(Math.abs(r.nextInt()) % 128);
                    //
                    // Skip quotation marks
                    //
                    if (Character.isLetter(data) == false) {
                       ctr--;
                    } else {
                       ret_str += String.valueOf(data);
                    }
                 }
                ret = ret_str;
                break;
            }
            // --------------------------------
            // VARBINARY
            // --------------------------------
            case VARBINARY: {
                int size = r.nextInt(maxSize) + 1;
                byte[] bytestr = new byte[size];
                r.nextBytes(bytestr);
                ret = bytestr;
                break;
            }
            // --------------------------------
            // TIMESTAMP
            // --------------------------------
            case TIMESTAMP: {
                long timestamp = r.nextInt((int)(VoltTypeUtil.DATE_STOP - VoltTypeUtil.DATE_START)) + VoltTypeUtil.DATE_START;
                ret = new TimestampType(Long.valueOf(timestamp * 1000));
                break;
            }
            // --------------------------------
            // INVALID
            // --------------------------------
            default:
                LOG.severe("ERROR: Unable to generate random value for invalid ValueType '" + type + "'");
        } // SWITCH
        return (ret);
    }

    private static final VoltType CAST_ORDER[] = {
        VoltType.STRING,
        VoltType.DECIMAL,
        VoltType.FLOAT,
        VoltType.TIMESTAMP,
        VoltType.BIGINT,
    };

    public static VoltType determineImplicitCasting(VoltType left, VoltType right) {
        //
        // Make sure both are valid
        //
        if (left == VoltType.INVALID || right == VoltType.INVALID) {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" + left + "' and '" + right + "' types");
        }
        // Check for NULL first, if either type is NULL the output is always NULL
        // XXX do we need to actually check for all NULL_foo types here?
        else if (left == VoltType.NULL || right == VoltType.NULL)
        {
            return VoltType.NULL;
        }
        //
        // No mixing of strings and numbers
        //
        else if ((left == VoltType.STRING && right != VoltType.STRING) ||
                (left != VoltType.STRING && right == VoltType.STRING))
        {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                        left + "' and '" + right + "' types");
        }

        // Allow promoting INTEGER types to DECIMAL.
        else if ((left == VoltType.DECIMAL || right == VoltType.DECIMAL) &&
                !(left.isExactNumeric() && right.isExactNumeric()))
        {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                        left + "' and '" + right + "' types");
        }
        //
        // The following list contains the rules that use for casting:
        //
        //    (1) If both types are a STRING, the output is always a STRING
        //        Note that up above we made sure that they do not mix strings and numbers
        //            Example: STRING + STRING -> STRING
        //    (2) If one type is a DECIMAL, the output is always a DECIMAL
        //        Note that above we made sure that DECIMAL only mixes with
        //        allowed types
        //    (3) Floating-point types take precedence over integers
        //            Example: FLOAT + INTEGER -> FLOAT
        //    (4) Specific types for floating-point and integer types take precedence
        //        over the more general types
        //            Example: MONEY + FLOAT -> MONEY
        //            Example: TIMESTAMP + INTEGER -> TIMESTAMP
        for (VoltType cast_type : CAST_ORDER) {
            //
            // If any one of the types is the current cast type, we'll use that
            //
            if (left == cast_type || right == cast_type)
            {
                return cast_type;
            }
        }

        // If we have INT types smaller than BIGINT
        // promote the output up to BIGINT
        if ((left == VoltType.INTEGER || left == VoltType.SMALLINT || left == VoltType.TINYINT) &&
                (right == VoltType.INTEGER || right == VoltType.SMALLINT || right == VoltType.TINYINT))
        {
            return VoltType.BIGINT;
        }

        // If we get here, we couldn't figure out what to do
        throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                    left + "' and '" + right + "' types");
    }

    /**
     * Returns a casted object of the input value string based on the given type
     * @throws ParseException
     */
    public static Object getObjectFromString(VoltType type, String value)
    throws ParseException
    {
        Object ret = null;
        switch (type) {
            // NOTE: All runtime integer parameters are actually Longs,so we will have problems
            // if we actually try to convert the object to one of the smaller numeric sizes

            // --------------------------------
            // INTEGERS
            // --------------------------------
            case TINYINT:
                //ret = Byte.valueOf(value);
                //break;
            case SMALLINT:
                //ret = Short.valueOf(value);
                //break;
            case INTEGER:
                //ret = Integer.valueOf(value);
                //break;
            case BIGINT:
                ret = Long.valueOf(value);
                break;
            // --------------------------------
            // FLOATS
            // --------------------------------
            case FLOAT:
                ret = Double.valueOf(value);
                break;
            // --------------------------------
            // STRINGS
            // --------------------------------
            case STRING:
                ret = value;
                break;

            case DECIMAL:
            case VARBINARY:
                if (value != null) {
                    throw new RuntimeException("Only NULL default values for DECIMAL " +
                            "and VARBINARY columns are supported right now");
                }
                break;
            // --------------------------------
            // TIMESTAMP
            // --------------------------------
            case TIMESTAMP: {
                // Support either long values (microseconds since epoch) or timestamp strings.
                try {
                    // Try to parse it as a long first.
                    ret = new TimestampType(Long.parseLong(value));
                }
                catch (NumberFormatException e) {
                    // It failed to parse as a long - parse it as a timestamp string.
                    Date date = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(value);
                    ret = new TimestampType(date.getTime() * 1000);
                }
                break;
            }
            // --------------------------------
            // INVALID
            // --------------------------------
            default:
                LOG.severe("ERROR: Unable to generate random value for invalid ValueType '" + type + "'");
        }
        return (ret);
    }

    public static String getSignatureForTable(String name, ArrayList<VoltType> schema) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        for (VoltType t : schema) {
            sb.append(t.getSignatureChar());
        }
        return sb.toString();
    }

    public static String getSignatureForTable(ArrayList<VoltType> schema) {
        StringBuilder sb = new StringBuilder();
        for (VoltType t : schema) {
            sb.append(t.getSignatureChar());
        }
        return sb.toString();
    }
}
