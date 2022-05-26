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

package org.voltdb.exportclient.decode;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Preconditions;

public final class EndpointExpander {

    final static int TABLE = 1;
    final static int PARTITION = 2;
    final static int GENERATION = 4;
    final static int DATE = 8;

    final static int HDFS_MASK = TABLE | PARTITION | GENERATION;

    static String DATE_FORMAT = "yyyyMMdd'T'HH";

    /**
     * Given a format string that may contain any of the following conversions
     * <ul>
     *   <li>%t which is replaced with the provided table name</li>
     *   <li>%p which is replaced with the provided partition id</li>
     *   <li>%g which is replaced with the provided generation</li>
     *   <li>%d which is replaced with the provided date</li>
     * </ul>
     * A percent sign may be used to escape another
     *
     * Requires:
     *  - template
     *  - date (if %d is in the template)
     *  - table name
     *  - template contains %t and %p
     *
     * Use the given parameters and place them where their respective
     * conversions are.
     *
     * @param tmpl format string
     * @param tn table name
     * @param p partition id
     * @param gn generation
     * @param dt date
     * @param tz timezone into which dates are formatted
     * @return expanded string with the applied parameter substitution conversions
     */
    public static String expand(String tmpl, String tn, int p, long gn, Date dt, TimeZone tz) {

        Preconditions.checkArgument(
                tmpl != null && !tmpl.trim().isEmpty(),
                "null or empty format string"
                );

        int conversionMask = conversionMaskFor(tmpl);
        boolean hasDateConversion = (conversionMask & DATE) == DATE;
        boolean hasTableConversion = (conversionMask & TABLE) == TABLE;

        Preconditions.checkArgument(
                (dt != null && hasDateConversion) || !hasDateConversion,
                "null date"
                );
        Preconditions.checkArgument(
                (tn != null && !tn.trim().isEmpty() && hasTableConversion) || !hasTableConversion,
                "null or empty table name"
                );

        SimpleDateFormat dateFormatter = null;
        if (hasDateConversion) {
            dateFormatter = new SimpleDateFormat(DATE_FORMAT);
            if (tz != null) {
                dateFormatter.setTimeZone(tz);
            }
        }

        StringBuilder sb = new StringBuilder(tmpl.length()+256).append(tmpl);
        for (int i = 0; i < sb.length();) {
            if (isEscaped(sb,i)) {
                sb.deleteCharAt(i);
                i += 1;
            } else if (isExpandable(sb,i)) {
                String r = "";
                switch(sb.charAt(i+1)) {
                case 't':
                    r = tn;
                    break;
                case 'g':
                    r = Long.toString(gn, Character.MAX_RADIX);
                    break;
                case 'd':
                    r = dateFormatter.format(dt);
                    break;
                case 'p':
                    r = Integer.toString(p);
                    break;
                }
                sb.replace(i,i+2,r);
                i += r.length();
            } else {
                i += 1;
            }
        }
        return sb.toString();
    }

    /**
     * Given a format string that may contain any of the following conversions
     * <ul>
     *   <li>%t which is replaced with the provided table name</li>
     *   <li>%p which is replaced with the provided partition id</li>
     *   <li>%g which is replaced with the provided generation</li>
     *   <li>%d which is replaced with the provided date</li>
     * </ul>
     * A percent sign may be used to escape another
     *
     * Requires:
     *  - template
     *  - date (if %d is in the template)
     *  - table name
     *  - template contains %t and %p
     *
     * Use the given parameters and place them where their respective
     * conversions are.
     *
     * @param tmpl format string
     * @param tn table name
     * @param p partition id
     * @param gn generation
     * @param dt date
     * @return expanded string with the applied parameter substitution conversions
     */
    public static String expand(String tmpl, String tn, int p, long gn, Date dt) {
        return expand(tmpl, tn, p, gn, dt, null);
    }


    /**
     * Given a format string that may contain any of the following conversions
     * <ul>
     *   <li>%t which is replaced with the provided table name</li>
     *   <li>%g which is replaced with the provided generation</li>
     * </ul>
     * A percent sign may be used to escape another
     *
     * Requires:
     *  - template
     *  - table name
     *  - template contains at least %t
     *
     * Use the given parameters and place them where their respective
     * conversions are.
     *
     * @param tmpl format string
     * @param tn table name
     * @param gn generation
     * @return expanded string with the applied parameter substitution conversions
     */
    public static String expand(String tmpl, String tn, long gn) {
        Preconditions.checkArgument(
                tmpl != null && !tmpl.trim().isEmpty(),
                "null or empty format string"
                );
        Preconditions.checkArgument(
                tn != null && !tn.trim().isEmpty(),
                "null or empty table name"
                );

        int conversionMask = conversionMaskFor(tmpl);

        Preconditions.checkArgument(
                (conversionMask & TABLE) == TABLE,
                "\"" + tmpl + "\" must contain at least the (%t)able conversion"
                );
        Preconditions.checkArgument(
                (conversionMask & ~(TABLE|GENERATION)) == 0,
                "\"" + tmpl + "\" may only contain the (%t)able, and the (%g)generation conversions"
                );

        StringBuilder sb = new StringBuilder(tmpl.length()+256).append(tmpl);
        for (int i = 0; i < sb.length();) {
            if (isEscaped(sb,i)) {
                sb.deleteCharAt(i);
                i += 1;
            } else if (isExpandable(sb,i)) {
                String r = "";
                switch(sb.charAt(i+1)) {
                case 't': r = tn; break;
                case 'g':
                    r = Long.toString(gn, Character.MAX_RADIX);
                    break;
                }
                sb.replace(i,i+2,r);
                i += r.length();
            } else {
                i += 1;
            }
        }
        return sb.toString();
    }

    static int conversionMaskFor(String sb) {
        if (sb == null || sb.trim().isEmpty()) return 0;
        int mask = 0;
        for (int i = 0; i < sb.length(); ) {
            if (isEscaped(sb,i)) {
                i += 2;
            } else if (isExpandable(sb,i)) {
                switch(sb.charAt(i+1)) {
                case 't': mask = mask | TABLE; break;
                case 'p': mask = mask | PARTITION; break;
                case 'g': mask = mask | GENERATION; break;
                case 'd': mask = mask | DATE; break;
                }
                i += 2;
            } else {
                i += 1;
            }
        }
        return mask;
    }

    /**
     * Checks whethere the given format string contains the (%d)ate
     * conversion
     * @param sb format string
     * @return true if yes, false if no
     */
    public static boolean hasDateConversion(String sb) {
        return (conversionMaskFor(sb) & DATE) == DATE;
    }

    /**
     * Verifies that given endpoint format string specifies all the required hdfs
     * conversions in the path portion of the endpoint.
     *
     * @param sb format string
     * @throws IllegalArgumentException when verification fails
     */
    public static void verifyForHdfsUse(String sb) throws IllegalArgumentException {
        Preconditions.checkArgument(
                sb != null && !sb.trim().isEmpty(),
                "null or empty hdfs endpoint"
                );

        int mask = conversionMaskFor(sb);
        boolean hasDateConversion = (mask & DATE) == DATE;

        Preconditions.checkArgument(
                (mask & HDFS_MASK) == HDFS_MASK,
                "hdfs endpoint \"" + sb
                + "\" must contain the (%t)able, the (%p)artition, and the (%g) generation conversions"
                );

        final String tn = "__IMPROBABLE_TABLE_NAME__";
        final int pn = Integer.MIN_VALUE;
        final long gn = Long.MIN_VALUE;
        final Date dt = new Date(0);
        final String fmtd = hasDateConversion ? new SimpleDateFormat(DATE_FORMAT).format(dt) : "";

        URI uri = URI.create(expand(sb, tn, pn, gn, dt));
        String path = uri.getPath();

        List<String> missing = new ArrayList<>();
        if (!path.contains(tn)) missing.add("%t");
        if (!path.contains(Integer.toString(pn))) missing.add("%p");
        if (!path.contains(Long.toString(gn,Character.MAX_RADIX))) missing.add("%g");
        if (hasDateConversion && !path.contains(fmtd)) missing.add("%d");

        if (!missing.isEmpty()) {
            String notInPath = Joiner.on(", ").join(missing);
            throw new IllegalArgumentException(
                    "hdfs enpoint \"" + sb
                    + "\" does not contain conversion(s) " + notInPath
                    + " in the path element of the URL");
        }
    }

    /**
     * Verifies that given endpoint format string specifies all the required batch mode
     * conversions.
     *
     * @param sb format string
     * @throws IllegalArgumentException when verification fails
     */
    public static void verifyForBatchUse(String sb) throws IllegalArgumentException {
        Preconditions.checkArgument(
                sb != null && !sb.trim().isEmpty(),
                "null or empty hdfs endpoint"
                );

        int mask = conversionMaskFor(sb);
        Preconditions.checkArgument(
                (mask & HDFS_MASK) == HDFS_MASK,
                "batch mode endpoint \"" + sb
                + "\" must contain the (%t)able, the (%p)artition, and the (%g) generation conversions"
                );
    }

    private static boolean isEscaped(CharSequence sb, int i) {
        return i+1 < sb.length() && sb.charAt(i) == '%' && sb.charAt(i+1) == '%';
    }

    private static boolean isExpandable(CharSequence sb, int i) {
        return i+1 < sb.length() && sb.charAt(i) == '%' && "dgpt".indexOf(sb.charAt(i+1)) >= 0;
    }

    public static void setDateFormatForTest(String format) {
        DATE_FORMAT = format;
    }

}
